package com.yahoo.gondola.impl;

import com.yahoo.gondola.Channel;
import com.yahoo.gondola.Gondola;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This channel implementation is used for functional testing and performance testing.
 * It allows members within one Gondola instance to communicate with each other via memory buffers.
 */
public class MemoryChannel implements Channel {
    final static Logger logger = LoggerFactory.getLogger(MemoryChannel.class);

    final Gondola gondola;
    final int memberId;
    final int peerId;

    // A unique id representing communications from member to peer
    String key;

    // A unique id representing communication from peer to member
    String rkey;

    // This lock is used to ensure that inputStreams and outputStreams are updated atomically for both key and rkey
    static final ReentrantLock lock = new ReentrantLock();
    static volatile Map<String, InputStream> inputStreams = new ConcurrentHashMap<>();
    static volatile Map<String, OutputStream> outputStreams = new ConcurrentHashMap<>();

    public MemoryChannel(Gondola gondola, int memberId, int peerId) {
        this.gondola = gondola;
        this.memberId = memberId;
        this.peerId = peerId;
        key = memberId + "-" + peerId;
        rkey = peerId + "-" + memberId;

        lock.lock();
        try {
            if (inputStreams.get(key) == null) {
                // Create member:in peer:out
                PipedInputStream in = new PipedInputStream(10000);
                PipedOutputStream out = new PipedOutputStream(in);
                inputStreams.put(key, in);
                outputStreams.put(rkey, out);

                // Create member:out peer:in
                in = new PipedInputStream(10000);
                out = new PipedOutputStream(in);
                inputStreams.put(rkey, in);
                outputStreams.put(key, out);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() {
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
    public void awaitOperational() {
        awaitOperational(false);
    }

    /**
     * See Channel.getInputStream().
     */
    @Override
    public InputStream getInputStream(InputStream in, boolean errorOccurred) {
        awaitOperational(errorOccurred);
        return inputStreams.get(key);
    }

    /**
     * See Channel.getOutputStream().
     */
    @Override
    public OutputStream getOutputStream(OutputStream out, boolean errorOccurred) {
        awaitOperational(errorOccurred);
        return outputStreams.get(key);
    }

    void awaitOperational(boolean errorOccurred) {
        while (inputStreams.get(key) == null) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
