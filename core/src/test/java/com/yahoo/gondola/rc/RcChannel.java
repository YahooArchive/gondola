/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.rc;

import com.yahoo.gondola.Channel;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.GondolaException;
import com.yahoo.gondola.core.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class RcChannel implements Channel {
    final static Logger logger = LoggerFactory.getLogger(RcChannel.class);

    final Gondola gondola;
    final int memberId;
    final int peerId;

    // A unique id representing communications from member to peer
    String key;

    // A map containing the reverse keys for any key
    static Map<String, String> rkeys = new ConcurrentHashMap<>();

    // This lock ensures that inputStreams and outputStreams are updated atomically for both key and rkey
    static final ReentrantLock lock = new ReentrantLock();
    static volatile Map<String, PipedInputStream> inputStreams = new ConcurrentHashMap<>();
    static volatile Map<String, PipedOutputStream> outputStreams = new ConcurrentHashMap<>();

    // When true, messages sent on this channel are buffered in the messages list
    boolean pauseDelivery;

    // Messages that have not been sent to the peer yet because pauseDelivery is true
    List<Message> messages = new ArrayList<>();

    // The output stream used by the member to send data to the peer.
    OutputStream out = new RcOutputStream();

    public RcChannel(Gondola gondola, int memberId, int peerId) {
        this.gondola = gondola;
        this.memberId = memberId;
        this.peerId = peerId;
        key = memberId + "-" + peerId;
        rkeys.put(key, peerId + "-" + memberId);

        lock.lock();
        try {
            if (inputStreams.get(key) == null) {
                createPipes(key);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void start() throws GondolaException {
    }

    @Override
    public boolean stop() {
        pauseDelivery = false;
        messages.forEach(Message::release);
        messages.clear();
        inputStreams.remove(key);
        outputStreams.remove(key);
        return true;
    }

    static void createPipes(String key) throws IOException {
        if (inputStreams.containsKey(key)) {
            return;
        }
        String rkey = rkeys.get(key);

        // Create member:in peer:out
        PipedInputStream in = new PipedInputStream(10000);
        PipedOutputStream out = new PipedOutputStream(in);
        in = inputStreams.put(key, in);
        if (in != null) {
            in.close();
        }
        out = outputStreams.put(rkey, out);
        if (out != null) {
            out.close();
        }

        // Create member:out peer:in
        in = new PipedInputStream(10000);
        out = new PipedOutputStream(in);
        in = inputStreams.put(rkey, in);
        if (in != null) {
            in.close();
        }
        out = outputStreams.put(key, out);
        if (out != null) {
            out.close();
        }
    }

    @Override
    public int getRemoteMemberId() {
        return peerId;
    }

    @Override
    public String getRemoteAddress() {
        return key;
    }

    /**
     * See Channel.isOperational().
     */
    @Override
    public boolean isOperational() {
        return inputStreams.get(key) != null;
    }

    /**
     * See Channel.awaitOperational().
     */
    @Override
    public void awaitOperational() throws InterruptedException {
        awaitOperational(false);
    }

    /**
     * See Channel.getInputStream().
     */
    @Override
    public InputStream getInputStream(InputStream in, boolean errorOccurred) throws InterruptedException {
        awaitOperational(errorOccurred);
        return inputStreams.get(key);
    }

    /**
     * See Channel.getOutputStream().
     */
    @Override
    public OutputStream getOutputStream(OutputStream out, boolean errorOccurred) throws InterruptedException {
        awaitOperational(errorOccurred);
        return this.out;
    }

    void awaitOperational(boolean errorOccurred) throws InterruptedException {
        while (inputStreams.get(key) == null) {
            Thread.sleep(1000);
        }
    }

    public void pauseDelivery(boolean pause) throws Exception {
        pauseDelivery = pause;
        if (!pauseDelivery) {
            // Send all saved messages
            for (Message m : messages) {
                OutputStream pipeOut = outputStreams.get(key);
                pipeOut.write(m.buffer, 0, m.size);
                m.release();
            }
            messages.clear();
        }
    }

    /**
     * **************** rc streams ******************
     */

    class RcOutputStream extends OutputStream {
        @Override
        public void write(byte[] b) throws IOException {
            throw new IllegalStateException("not implemented");
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            Message message = gondola.getMessagePool().checkout();
            try {
                message.read(b, off, len);
                if (pauseDelivery) {
                    messages.add(message);
                } else {
                    OutputStream pipeOut = outputStreams.get(key);
                    pipeOut.write(b, off, len);
                    pipeOut.flush();
                    message.release();
                }
            } catch (Exception e) {
                message.release();
                if (!"Read end dead".equals(e.getMessage())) {
                    throw new IOException("key=" + key, e);
                }
            }
        }

        @Override
        public void write(int b) throws IOException {
            throw new IllegalStateException("not implemented");
        }
    }
}
