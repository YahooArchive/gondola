/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.demo;

import com.yahoo.gondola.Shard;
import com.yahoo.gondola.Command;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.Role;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * The core business logic of demo service.
 */
public class DemoService {
    Logger logger = LoggerFactory.getLogger(DemoService.class);

    Gondola gondola;

    // The map holding all the entries
    Map<String, String> entries = new ConcurrentHashMap<>();

    // Gondola shard, used for replication
    Shard shard;

    ChangeLogProcessor clProcessor;

    public DemoService(Gondola gondola) throws Exception {
        this.gondola = gondola;
        shard = gondola.getShardsOnHost().get(0);

        clProcessor = new ChangeLogProcessor();
        clProcessor.start();
    }

    /**
     * Returns the value stored at the specified key.
     *
     * @param key
     * @return The non-null value of the key
     * @throws NotFoundException
     */
    public String getValue(String key) throws NotFoundException, NotLeaderException {
        if (shard.getLocalRole() != Role.LEADER) {
            throw new NotLeaderException();
        }
        if (!entries.containsKey(key)) {
            throw new NotFoundException();
        }
        String value = entries.get(key);
        logger.info(String.format("[%s] Get key %s: %s", gondola.getHostId(), key, value));
        return value;
    }

    /**
     * Commits the entry to Raft log. The entries map is not updated; it is updated by the Replicator thread.
     * @param key The non-null key
     * @param value The non-null value
     */
    public void putValue(String key, String value) throws NotLeaderException {
        if (key.indexOf(" ") >= 0) {
            throw new IllegalArgumentException("The key must not contain spaces");
        }
        try {
            Command command = shard.checkoutCommand();
            byte[] bytes = (key + " " + value).getBytes(); // TODO implement better separator
            command.commit(bytes, 0, bytes.length);
            logger.info(String.format("[%s] Put key %s=%s", gondola.getHostId(), key, value));
        } catch (com.yahoo.gondola.NotLeaderException e) {
            logger.info(String.format("Failed to put %s/%s because not a leader", key, value));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the applied index.
     *
     * @return applied index.
     */
    public int getAppliedIndex() {
       return clProcessor.getAppliedIndex();
    }

    /**
     * Background thread that continuously reads committed commands from the Gondola shard, and updates the entries
     * map. TODO: prevent reads until the map is fully updated.
     */
    public class ChangeLogProcessor extends Thread {
        int appliedIndex = 0;
        List<Consumer<String>> listeners = new ArrayList<>();

        @Override
        public void run() {
            String string;
            while (true) {
                try {
                    string = shard.getCommittedCommand(appliedIndex + 1).getString();
                    appliedIndex++;
                    logger.info("[{}] Executing command {}: {}", gondola.getHostId(),
                                appliedIndex, string);
                    String[] pair = string.split(" ", 2);
                    if (pair.length == 2) {
                        entries.put(pair[0], pair[1]);
                    }
                } catch (Throwable e) {
                    logger.error(e.getMessage(), e);
                    break;
                }
            }
        }

        public int getAppliedIndex() {
            return appliedIndex;
        }
    }

    /**
     * Called after all changes from the Raft log have been applied and before requests
     * are accepted.
     */
    public void beforeServing() {
        // In this demo application, there's no need to update internal state
        logger.info("[{}] Ready", gondola.getHostId());
    }

    /**
     * Thrown when the key for a GET request does not exist.
     */
    public static class NotFoundException extends Throwable {
    }

    /**
     * Thrown when the current node is not the leader.
     */
    public static class NotLeaderException extends Throwable {
    }
}
