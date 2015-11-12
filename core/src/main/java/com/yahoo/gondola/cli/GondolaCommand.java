/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.cli;

import com.yahoo.gondola.*;
import com.yahoo.gondola.core.CoreMember;
import com.yahoo.gondola.impl.NastyNetwork;
import com.yahoo.gondola.impl.NastyStorage;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// TODO: will eventually be command line tool for users to quickly bring up a gondola cluster in their 
// environment and do some quick performance tests. They would 
// 1. create a config file and 2. start this command on the different hosts.
// Right now this is just a quick and dirty test command to test the prototype.
public class GondolaCommand {
    final static Logger logger = LoggerFactory.getLogger(GondolaCommand.class);

    Cluster cluster;
    Config config;
    Gondola gondola;

    AtomicLong waitTime = new AtomicLong();
    AtomicLong requests = new AtomicLong();

    // Parameters
    boolean serverMode = false;
    int port = 1099;
    String hostId;
    String clusterId;
    int delay = 1000;
    File configFile = new File("conf/gondola-sample.conf");

    int maxWorkers = 1024;
    int numWorkers = 0;
    BlockingQueue<Socket> socketQueue = new ArrayBlockingQueue<>(maxWorkers);
    AtomicInteger availableWorkers = new AtomicInteger();

    // Config variables
    int maxCommandSize;
    boolean tracing;

    CoreMember.Latency latency = new CoreMember.Latency();

    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure("conf/log4j.properties");
        new GondolaCommand(args);
    }

    public void printUsage() {
        // Stop is implemented in the shell script
        System.out.println("Usage: gondola.sh -hostid <host-id> -clusterid <cluster-id> [-port <port>] [-workers <num>] [start|stop]");
        if (config != null) {
            System.out.println("    Available host ids: " + config.getHostIds());
        }
        System.exit(1);
    }

    public GondolaCommand(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
        }

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("start")) {
                serverMode = true;
            }
            if (args[i].equals("-port")) {
                if (args.length == i - 1) {
                    printUsage();
                }
                port = Integer.parseInt(args[++i]);
            }
            if (args[i].equals("-hostid")) {
                if (args.length == i - 1) {
                    printUsage();
                }
                hostId = args[++i];
            }
            if (args[i].equals("-clusterid")) {
                if (args.length == i - 1) {
                    printUsage();
                }
                clusterId = args[++i];
            }
            if (args[i].equals("-workers")) {
                if (args.length == i - 1) {
                    printUsage();
                }
                numWorkers = Integer.parseInt(args[++i]);
            }
            if (args[i].equals("-delay")) {
                if (args.length == i - 1) {
                    printUsage();
                }
                delay = Integer.parseInt(args[++i]);
            }
            if (args[i].equals("-config")) {
                if (args.length == i - 1) {
                    printUsage();
                }
                configFile = new File(args[++i]);
            }
        }
        config = new Config(configFile);
        if (hostId == null || clusterId == null) {
            printUsage();
        }

        maxCommandSize = config.getInt("raft.command_max_size");
        tracing = config.getBoolean("tracing.cli.command");

        // Initialize the gondola system
        try {
            gondola = new Gondola(config, hostId);
            gondola.start();
            cluster = gondola.getCluster(clusterId);
        } catch (Exception e) {
            logger.error("Could not initialize gondola", e);
            System.exit(1);
        }

        if (serverMode) {
            System.out.println("Server-mode");
            new Acceptor().start();
        } else {
            // Temporary test mode
            Map<Integer, Integer> values = new HashMap<>();
            // Create writer threads
            for (int i = 0; i < numWorkers; i++) {
                values.put(i, 0);
                new Writer(hostId, i).start();
            }

            // Create reader
            //new Reader().start();
            // Print out some RPS stats
            while (true) {
                long start = requests.get();
                Thread.sleep(5000);
                double avgLatency = 1.0 * waitTime.get() / requests.get();
                if (cluster.getLocalMember().isLeader()) {
                    logger.info(String.format("commits: %.2f/s  latency: %.2f ms (%.2fms)",
                                              (requests.get() - start) / 5.0, avgLatency, latency.get()));
                }
                waitTime.set(0);
                requests.set(0);
            }
        }

    }

    // Background thread to get committed commands from gondola
    class Reader extends Thread {
        public Reader() {
            setName("CommandReader-");
        }

        @Override
        public void run() {
            byte[] buffer = new byte[maxCommandSize];

            int index = 0;
            try {
                index = Math.max(1, cluster.getLastSavedIndex());
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            int start = index;
            Command command = null;
            long startTs = System.currentTimeMillis();
            while (true) {
                try {
                    command = cluster.getCommittedCommand(index, 5000);
                    System.arraycopy(command.getBuffer(), 0, buffer, 0, command.getSize());
                    command.release();
                    index++;
                } catch (IndexNotFoundException e) {
                    index = e.getEarliestIndex();
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    logger.info(e.getMessage());
                } catch (Exception e) {
                    e.printStackTrace();
                }

                long now = System.currentTimeMillis();
                if (now - startTs > 5000) {
                    logger.info(String.format("reads: %.2f/s", (index - start) / 5.0));
                    start = index;
                    startTs = now;
                }
            }
        }
    }

    static AtomicInteger ccounter = new AtomicInteger();
    class Writer extends Thread {
        int id;

        Writer(String hostId, int id) {
            setName("worker-" + hostId + "-" + id);
            this.id = id;
        }

        public void run() {
            int value = 0;
            byte[] buffer = new byte[128];
            ByteBuffer bbuffer = ByteBuffer.wrap(buffer);
            long wait = 0;
            int count = 0;

            while (true) {
                try {
                    if (cluster.getLocalMember().isLeader()) {
                        Command command = cluster.checkoutCommand();
                        bbuffer.clear();
                        bbuffer.putInt(id);
                        bbuffer.putInt(value);

                        long start = System.currentTimeMillis();
                        int c = ccounter.incrementAndGet();
                        latency.head(c);
                        command.commit(buffer, 0, 128);
                        latency.tail(c);
                        waitTime.addAndGet(System.currentTimeMillis() - start);
                        requests.incrementAndGet();

                        value++;
                        command.release();
                        if (delay > 0) {
                            Thread.sleep(delay);
                        }
                    } else {
                        Thread.sleep(1000);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * **************************** server mode **************************
     */

    class Acceptor extends Thread {
        public Acceptor() {
            setName("RemoteInterfaceAcceptor-" + gondola.getHostId());
            new RemoteInterface().start();
        }

        public void run() {
            while (true) {
                logger.info("Listening " + hostId + " " + port);
                try (ServerSocket listener = new ServerSocket(port)) {
                    while (true) {
                        Socket socket = listener.accept();
                        socket.setTcpNoDelay(true);
                        if (tracing) {
                            logger.info("[{}] GondolaCommand socket accept from {} ({})", hostId,
                                    socket.getInetAddress());
                        }

                        // Create more remote interface threads if needed
                        if (availableWorkers.get() <= socketQueue.size() && numWorkers < maxWorkers) {
                            if (tracing) {
                                logger.info("[{}] Creating remote interface worker: avail workers={} socketQ={} numWorkers={}",
                                            hostId, availableWorkers.get(), socketQueue, numWorkers);
                            }
                            new RemoteInterface().start();
                            numWorkers++;
                        }
                        socketQueue.put(socket);
                    }
                } catch (BindException e) {
                    logger.error(e.getMessage(), e);
                    Runtime.getRuntime().halt(0);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);

                    // Small delay to avoid a spin loop
                    try {
                        Thread.sleep(1000);
                    } catch (Exception e2) {
                        logger.error(e2.getMessage(), e2);
                    }
                }
            }
        }
    }

    class RemoteInterface extends Thread {
        Socket socket;

        public RemoteInterface() {
            setName("RemoteInterfaceWorker-" + gondola.getHostId());
        }

        public void run() {
            while (true) {
                try {
                    availableWorkers.incrementAndGet();
                    socket = socketQueue.take();
                    availableWorkers.decrementAndGet();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                try (
                        InputStream in = socket.getInputStream();
                        OutputStream out = socket.getOutputStream();
                        BufferedReader rd = new BufferedReader(new InputStreamReader(in));
                        BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(out));
                ) {
                    try {
                        String line;
                        while ((line = rd.readLine()) != null) {
                            if (line.length() == 0) {
                                continue;
                            }
                            String result = processCommand(line);
                            wr.write(result + "\n");
                            wr.flush();
                            if (line.startsWith("q")) {
                                Runtime.getRuntime().halt(0);
                            }
                        }
                    } catch (NotLeaderException e) {
                        wr.write(String.format("ERROR: Not leader - leader is %s", e.getLeaderAddress()));
                    } catch (Exception e) {
                        wr.write("ERROR: Unhandled exception: " + e.getMessage() + "\n");
                        logger.error(e.getMessage(), e);
                    }
                } catch (Exception e) {
                    logger.error("GondolaCommand connection closed", e);
                }
            }
        }

        String processCommand(String line) throws Exception {
            String[] args = line.split(" ");
            if (args.length == 0) {
                return "ERROR: No command";
            }
            
            switch (args[0]) {
                case "c":
                    // commit a command
                    if (args.length < 2) {
                        return "ERROR: c <command>";
                    }
                    Command command = cluster.checkoutCommand();
                    try {
                        byte[] buffer = line.substring(2).getBytes("UTF-8");
                        if (buffer.length > command.getCapacity()) {
                            String msg = String.format("ERROR command too long. size=%d, max=%d",
                                    buffer.length, command.getCapacity());
                            logger.error(msg);
                            return msg;
                        }
                        command.commit(buffer, 0, buffer.length);
                    } finally {
                        command.release();
                    }
                    return String.format("SUCCESS: index %d", command.getIndex());
                case "F":
                    Role role = cluster.getLocalRole();
                    if (cluster.getLocalRole() == Role.LEADER) {
                        logger.info("This member is already a leader");
                    } else {
                        logger.info("Forcing {} to be a leader...", role);
                        int timeout = -1;
                        if (args.length >= 2) {
                            timeout = Integer.parseInt(args[1]);
                        }
                        cluster.forceLeader(timeout);
                    }
                    return "SUCCESS: This member is now " + cluster.getLocalRole();
                case "m":
                    // return the current role
                    return String.format("SUCCESS: %s", cluster.getLocalRole());
                case "g":
                    // get command by index
                    if (args.length < 2) {
                        return "ERROR: g <index> <timeout>";
                    }
                    int index = Integer.parseInt(args[1]);
                    int timeout = args.length > 2 ? Integer.parseInt(args[2]) : -1;
                    command = cluster.getCommittedCommand(index, timeout);
                    try {
                        return String.format("SUCCESS: %s", command.getString());
                    } finally {
                        command.release();
                    }
                case "s":
                    // Status
                    LogEntry le = gondola.getStorage().getLastLogEntry(cluster.getLocalMember().getMemberId());
                    int savedIndex = 0;
                    if (le != null) {
                        savedIndex = le.index;
                        le.release();
                    }
                    return String.format("SUCCESS: Mode=%s, commitIndex=%d, savedIndex=%d",
                            cluster.getLocalRole(), cluster.getLastSavedIndex(), savedIndex);
                case "n":
                    if (args.length < 2) {
                        return "ERROR: n <on|off>";
                    }
                    // Enable/disable nasty mode
                    if (gondola.getStorage() instanceof NastyStorage) {
                        NastyStorage ns = (NastyStorage) gondola.getStorage();
                        ns.enable(args[1].equals("on"));
                        logger.info("Nasty storage " + ns.isEnabled());
                    }
                    if (gondola.getNetwork() instanceof NastyNetwork) {
                        NastyNetwork nn = (NastyNetwork) gondola.getNetwork();
                        nn.enable(args[1].equals("on"));
                        logger.info("Nasty network " + nn.isEnabled());
                    }
                    return "SUCCESS: Nasty mode " + args[1];
                case "q":
                    // Exit immediately
                    le = gondola.getStorage().getLastLogEntry(cluster.getLocalMember().getMemberId());
                    if (le != null) {
                        logger.info("lastTerm={}, lastIndex={}", le.term, le.index);
                    }
                    logger.info("Exiting");
                    return "SUCCESS: exiting";
                default:
                    String msg = String.format("ERROR: Invalid command: %s", line);
                    logger.error(msg);
                    return msg;
            }
        }
    }
}
