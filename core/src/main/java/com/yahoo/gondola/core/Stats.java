/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The type Stats.
 */
public class Stats implements StatsMBean {
    final static Logger logger = LoggerFactory.getLogger(Stats.class);

    AtomicInteger hello = new AtomicInteger();
    AtomicInteger appendEntryRequest = new AtomicInteger();
    AtomicInteger appendEntryBatch = new AtomicInteger();
    AtomicInteger appendEntryReply = new AtomicInteger();
    AtomicInteger requestVoteRequest = new AtomicInteger();
    AtomicInteger requestVoteReply = new AtomicInteger();
    AtomicInteger emptyCommandPool = new AtomicInteger();

    AtomicInteger sentMessages = new AtomicInteger();
    AtomicLong sentBytes = new AtomicLong();
    float sentMessagesRps;

    AtomicInteger incomingMessages = new AtomicInteger();
    AtomicLong incomingBytes = new AtomicLong();
    AtomicLong incomingQueueFull = new AtomicLong();
    float incomingMessagesRps;

    AtomicInteger savedCommands = new AtomicInteger();
    AtomicLong savedBytes = new AtomicLong();
    float savedCommandsRps;

    public Stats() {
        new RpsCalculator().start();
    }

    /* ******************* get ****************** */

    public int getAppendEntryRequest() {
        return appendEntryRequest.get();
    }

    public int getAppendEntryBatch() {
        return appendEntryBatch.get();
    }

    public int getAppendEntryReply() {
        return appendEntryReply.get();
    }

    public int getRequestVoteRequest() {
        return requestVoteRequest.get();
    }

    public int getRequestVoteReply() {
        return requestVoteReply.get();
    }

    public int getEmptyCommandPool() {
        return emptyCommandPool.get();
    }

    public int getSentMessages() {
        return sentMessages.get();
    }

    public long getSentBytes() {
        return sentBytes.get();
    }

    public int getIncomingMessages() {
        return incomingMessages.get();
    }

    public long getIncomingBytes() {
        return incomingBytes.get();
    }

    public long getIncomingQueueFull() {
        return incomingQueueFull.get();
    }

    @Override
    public int getSavedCommands() {
        return savedCommands.get();
    }

    @Override
    public long getSavedBytes() {
        return savedBytes.get();
    }

    /* ******************* update ****************** */

    @Override
    public void hello() {
        hello.incrementAndGet();
    }

    @Override
    public void appendEntryRequest() {
        appendEntryRequest.incrementAndGet();
    }

    @Override
    public void appendEntryBatch() {
        appendEntryBatch.incrementAndGet();
    }

    @Override
    public void appendEntryReply() {
        appendEntryReply.incrementAndGet();
    }

    @Override
    public void requestVoteRequest() {
        requestVoteRequest.incrementAndGet();
    }

    @Override
    public void requestVoteReply() {
        requestVoteReply.incrementAndGet();
    }

    @Override
    public void emptyCommandPool() {
        emptyCommandPool.incrementAndGet();
    }

    @Override
    public void sentMessage(int bytes) {
        sentMessages.incrementAndGet();
        sentBytes.addAndGet(bytes);
    }

    @Override
    public void incomingMessage(int bytes) {
        incomingMessages.incrementAndGet();
        incomingBytes.addAndGet(bytes);
    }

    @Override
    public void incomingQueueFull() {
        incomingQueueFull.incrementAndGet();
    }

    @Override
    public void savedCommand(int bytes) {
        savedCommands.incrementAndGet();
        savedBytes.addAndGet(bytes);
    }

    class RpsCalculator extends Thread {
        int period = 10000;

        public RpsCalculator() {
            setName("RpsCalcuator-");
            setDaemon(true);
        }
        public void run() {
            int inm = incomingMessages.get();
            int outm = sentMessages.get();
            int sc = savedCommands.get();

            while (true) {
                try {
                    int inm2 = incomingMessages.get();
                    incomingMessagesRps = (inm2 - inm) * 1000.0f / period;
                    inm = inm2;

                    int outm2 = sentMessages.get();
                    sentMessagesRps = (outm2 - outm) * 1000.0f / period;
                    outm = outm2;

                    int sc2 = savedCommands.get();
                    savedCommandsRps = (sc2 - sc) * 1000.0f / period;
                    sc = sc2;

                    Thread.sleep(period);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }
}
