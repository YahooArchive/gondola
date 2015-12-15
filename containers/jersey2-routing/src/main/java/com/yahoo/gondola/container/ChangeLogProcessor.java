/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.yahoo.gondola.Command;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.GondolaException;
import com.yahoo.gondola.Shard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * The type Change log processor.
 */
public class ChangeLogProcessor {

    private final Map<String, RoutingService> services;
    private Gondola gondola;
    private Map<String, ChangeLogProcessorThread> threads = new HashMap<>();
    private static Logger logger = LoggerFactory.getLogger(ChangeLogProcessor.class);


    /**
     * Instantiates a new Change log processor.
     *
     * @param gondola  the gondola
     * @param services the routing services
     */
    public ChangeLogProcessor(Gondola gondola, Map<String, RoutingService> services) {
        this.gondola = gondola;
        this.services = services;
        gondola.getConfig().getShardIds(gondola.getHostId()).forEach(this::createThread);
    }

    /**
     * The type Change log processor thread.
     */
    class ChangeLogProcessorThread extends Thread {

        int appliedIndex = 0;
        private int retryCount = 0;
        private Shard shard;
        private String shardId;
        private String hostId;
        private int memberId;
        private ChangeLogConsumer changeLogConsumer;
        boolean reset = false;

        public ChangeLogProcessorThread(String shardId) {
            setName("ChangeLogProcessor");
            this.shardId = shardId;
            this.hostId = gondola.getHostId();
            this.memberId = gondola.getShard(shardId).getLocalMember().getMemberId();
            this.changeLogConsumer = services.get(shardId).provideChangeLogConsumer();
        }

        public void run() {
            Command command;
            shard = gondola.getShard(shardId);
            while (true) {
                try {
                    command = shard.getCommittedCommand(appliedIndex + 1);
                    if (changeLogConsumer != null) {
                        changeLogConsumer.applyLog(shardId, command);
                    }
                    appliedIndex++;
                } catch (GondolaException e) {
                    logger.info("[{}-{}] Error while get gondola command, appliedIndex={}, error={}",
                                hostId, memberId, appliedIndex, e.getMessage());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        if (handleInterrupt()) {
                            return;
                        }
                    }
                } catch (InterruptedException e) {
                    if (handleInterrupt()) {
                        return;
                    }
                } catch (Throwable e) {
                    logger.error(e.getMessage(), e);
                    if (++retryCount == 3) {
                        logger
                            .error("[{}-{}] Max retry count reached, exit..", hostId, memberId);
                        return;
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        if (handleInterrupt()) {
                            return;
                        }
                    }
                }
            }
        }

        private boolean handleInterrupt() {
            if (reset) {
                logger.info("[{}-{}] ChangeLogProcessor reset appliedIndex to 0", hostId, memberId);
                reset = false;
                appliedIndex = 0;
                return false;
            } else {
                logger.warn("[{}-{}] ChangeLogProcessor interrupted, exit..", hostId, memberId);
                return true;
            }
        }
    }

    private void createThread(String shardId) {
        ChangeLogProcessorThread thread = new ChangeLogProcessorThread(shardId);
        threads.put(shardId, thread);
    }

    /**
     * Gets applied index.
     *
     * @param shardId the shard id
     * @return the applied index
     */
    public int getAppliedIndex(String shardId) {
        return threads.get(shardId).appliedIndex;
    }

    /**
     * Stop.
     */
    public void stop() {
        com.yahoo.gondola.core.Utils.stopThreads(new ArrayList<>(threads.values()));
    }

    public void start() {
        threads.values().forEach(ChangeLogProcessorThread::start);
    }

    /**
     * Reset
     */
    public void reset(String shardId) {
        ChangeLogProcessorThread t = threads.get(shardId);
        t.reset = true;
        t.interrupt();
    }

    /**
     * Change log consumer functional interface.
     */
    @FunctionalInterface
    public interface ChangeLogConsumer {

        void applyLog(String shardId, Command command);
    }
}
