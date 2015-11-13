/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.impl;

import com.yahoo.gondola.Channel;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.Network;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is responsible for creating connections to other members based on network sockets.
 * It accepts connection requests from remote members and provides these to SocketChannel objects.
 * <p>
 * The client first calls createChannel() to create a connection between it and a remote
 * member. When this class receives a socket connection request, it initializes it and
 * gives it to the channel object via setSocket().
 * <p>
 * To avoid dealing with a race condition that can happen if both the local and remote members
 * can establish connections, by convention, the member whose id is greater is the one who
 * is responsible for establishing the call.
 * <p>
 * There is a simple text-based handshake to identify the calling members when the socket
 * connection is first established:
 * callee: hello from <host-id>
 * caller: call from A to B
 * callee: ok
 * <p>
 * callee: hello from <host-id>
 * caller: B should call A
 * callee: ok
 */
public class SocketNetwork implements Network, Observer {
    final static Logger logger = LoggerFactory.getLogger(SocketNetwork.class);

    final Gondola gondola;
    final String hostId;

    List<Channel> channels = new CopyOnWriteArrayList<>();

    // Config variables
    static boolean networkTracing;
    static int connTimeout;

    // Used to interrupt the acceptor thread. IO methods throw InterruptedIOException when interrupted and when closed.
    // This variable allows the acceptor thread to distinguish between the two. If the generation is updated, then
    // an interrupt occured.
    int generation = 0;

    // List of threads running in this class
    List<Thread> threads = new ArrayList<>();

    public SocketNetwork(Gondola gondola, String hostId) throws SocketException {
        this.gondola = gondola;
        this.hostId = hostId;
        gondola.getConfig().registerForUpdates(this);

        InetSocketAddress address = gondola.getConfig().getAddressForHost(hostId);
        if (!isLocalAddress(address.getAddress())) {
            throw new IllegalStateException(address.getHostName() + " is not a local address");
        }

        // Check whether another process is already using this address
        if (isActive(getAddress())) {
            throw new IllegalStateException(String.format("Another process is actively listening to %s",
                    getAddress()));
        }
    }

    /*
     * Called at the time of registration and whenever the config file changes.
     */
    @Override
    public void update(Observable obs, Object arg) {
        Config config = (Config) arg;
        networkTracing = config.getBoolean("tracing.network");
        connTimeout = config.getInt("network_socket.connect_timeout");
    }

    @Override
    public void start() {
        threads.add(new Acceptor());
        threads.forEach(t -> t.start());
    }

    @Override
    public void stop() {
        generation++;
        threads.forEach(t -> t.interrupt());
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        threads.clear();
    }

    /**
     * ***************** methods ******************
     */

    @Override
    public Channel createChannel(int fromMemberId, int toMemberId) {
        SocketChannel channel = new SocketChannel(gondola, fromMemberId, toMemberId);
        channels.add(channel);
        return channel;
    }

    @Override
    public String getAddress() {
        InetSocketAddress addr = gondola.getConfig().getAddressForHost(hostId);
        return String.format("%s:%d", addr.getHostString(), addr.getPort());
        /*
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            logger.error(e.getMessage(), e);
            return "Unknown";
        }
        */
    }

    @Override
    public boolean isActive(String address) {
        String[] parts = address.split(":");
        InetSocketAddress addr = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
        try {
            Socket socket = new Socket();
            socket.connect(addr, connTimeout);
            socket.close();
            // Process is alive enough to respond to a connect request
            return true;
        } catch (Exception e) {
            // Can't connect to the process so probably not alive
            logger.info("{} is not active: {}", address, e.getMessage());
        }
        return false;
    }

    @Override
    public List<Channel> getChannels() {
        return channels;
    }

    /*
     * This thread waits for connections from other members.
     */
    class Acceptor extends Thread {
        Acceptor() {
            setName("Acceptor-" + gondola.getHostId());
        }

        public void run() {
            InetSocketAddress addr = gondola.getConfig().getAddressForHost(hostId);
            ServerSocket listener = null;
            int gen = generation;

            while (true) {
                try {
                    listener = new ServerSocket(addr.getPort());
                    while (true) {
                        Socket socket = listener.accept();
                        socket.setTcpNoDelay(true);
                        logger.info("[{}] Socket accept from {}", gondola.getHostId(), socket.getInetAddress());

                        // Creates a separate thread to handle the handshake, to avoid hangs, etc.
                        new Initializer(socket).start();
                    }
                } catch (Exception e) {
                    if (gen < generation) {
                        return;
                    }
                    logger.error(e.getMessage(), e);

                    try {
                        // Small delay to avoid a spin loop
                        Thread.sleep(1000);
                    } catch (InterruptedException e2) {
                        return;
                    } catch (Exception e2) {
                        logger.error(e2.getMessage(), e2);
                    }
                } finally {
                    if (listener != null) {
                        try {
                            listener.close();
                        } catch (Exception e2) {
                            logger.error(e2.getMessage(), e2);
                        }
                    }
                }
            }
        }
    }

    /*
     * This thread initates the handshake with the remote member.
     * If successful, offers the socket to the channel, making it operational.
     */
    class Initializer extends Thread {
        Socket socket;

        Initializer(Socket socket) {
            this.socket = socket;
        }

        public void run() {
            try {
                Hello hello = new Hello(gondola.getHostId(), socket.getInputStream(), socket.getOutputStream());
                hello.incoming();

                // Find the conn that matches the communicating members
                SocketChannel channel = null;
                for (Channel c : channels) {
                    SocketChannel ch = (SocketChannel) c;
                    if (ch.memberId == hello.toMemberId && ch.peerId == hello.fromMemberId) {
                        channel = ch;
                        break;
                    }
                }

                // The target member id is not known so reject the connection request
                if (channel == null) {
                    logger.info("[{}] Connection request from {} to {} rejected because the channel is not registered",
                            gondola.getHostId(), hello.fromMemberId, hello.toMemberId);
                    socket.close();
                } else {
                    hello.ok();

                    if (hello.callFrom) {
                        // Make the socket available to the channel
                        channel.setSocket(socket, hello.in, hello.out);
                    } else {
                        ((SocketChannel) channel).retry();
                    }
                }
            } catch (Exception e) {
                if (networkTracing) {
                    logger.warn(e.getMessage(), e);
                } else {
                    logger.warn(e.getMessage());
                }
            }
        }
    }

    private boolean isLocalAddress(InetAddress addr) {
        // Check if the address is a valid special local or loop back
        if (addr.isAnyLocalAddress() || addr.isLoopbackAddress()) {
            return true;
        }

        // Check if the address is defined on any interface
        try {
            return NetworkInterface.getByInetAddress(addr) != null;
        } catch (SocketException e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    static Pattern callFromPtrn = Pattern.compile("call from (\\d+) to (\\d+)");
    static Pattern callBackPtrn = Pattern.compile("call back (\\d+) from (\\d+)");

    /**
     * protocol:
     * caller: hello from host1
     * call from 81 to 82
     * ok
     */
    public static class Hello {
        String hostId;
        InputStream in;
        OutputStream out;
        int fromMemberId;
        int toMemberId;
        boolean callFrom;

        Hello(String hostId, InputStream in, OutputStream out) {
            this.hostId = hostId;
            this.in = in;
            this.out = out;
        }

        /**
         * Gets the calling member ids.
         */
        void incoming() throws Exception {
            // Greeting
            writeLine(String.format("hello from %s", hostId));

            // Get ids
            String line = readLine();
            if (networkTracing) {
                logger.info("[{}] {}", hostId, line);
            }
            Matcher matcher = callFromPtrn.matcher(line);
            if (matcher.find()) {
                callFrom = true;
                fromMemberId = Integer.parseInt(matcher.group(1));
                toMemberId = Integer.parseInt(matcher.group(2));
                if (fromMemberId <= toMemberId) {
                    throw new IllegalStateException("From id must be > to id: " + line);
                }
            } else {
                matcher = callBackPtrn.matcher(line);
                if (matcher.find()) {
                    fromMemberId = Integer.parseInt(matcher.group(1));
                    toMemberId = Integer.parseInt(matcher.group(2));
                } else {
                    throw new IllegalStateException("Invalid message: " + line);
                }
            }
        }

        /**
         * Sends ok to the remote member. Sent after a call to incoming.
         */
        void ok() throws Exception {
            // Success
            writeLine("ok");
        }

        void callFrom(int fromMemberId, int toMemberId) throws Exception {
            assert fromMemberId > toMemberId;

            outgoing(true, fromMemberId, toMemberId);
        }

        void callBack(int fromMemberId, int toMemberId) throws Exception {
            assert fromMemberId < toMemberId;

            outgoing(false, fromMemberId, toMemberId);
        }

        void outgoing(boolean callFrom, int fromMemberId, int toMemberId) throws Exception {
            // Get greeting from remote
            String line = readLine();
            if (networkTracing) {
                logger.info("[{}] {}", hostId, line);
            }
            if (!line.startsWith("hello from ")) {
                throw new IllegalStateException("Invalid response: " + line);
            }

            // Send target id to remote
            if (callFrom) {
                writeLine(String.format("call from %d to %d", fromMemberId, toMemberId));
            } else {
                writeLine(String.format("call back %d from %d", fromMemberId, toMemberId));
            }

            // Get success from remote
            line = readLine();
            if (networkTracing) {
                logger.info("[{}] {}", hostId, line);
            }
            if (!line.equals("ok")) {
                throw new IllegalStateException("Invalid response: " + line);
            }
        }

        /**
         * Does not return the \n
         */
        String readLine() throws Exception {
            StringBuilder sb = new StringBuilder();
            int c;
            while ((c = in.read()) != '\n') {
                if (c < 0) {
                    throw new IOException("End-of-file");
                }
                sb.append((char) c);
            }
            return sb.toString().trim();
        }

        /**
         * Appends a \n
         */
        void writeLine(String line) throws Exception {
            for (int i = 0; i < line.length(); i++) {
                out.write(line.charAt(i));
            }
            out.write('\n');
        }
    }
}
