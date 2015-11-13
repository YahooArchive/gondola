/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.demo;

import com.yahoo.gondola.Cluster;
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

    // The map holding all the entries
    Map<String, String> entries = new ConcurrentHashMap<>();

    // Gondola cluster, used for replication
    Cluster cluster;

    ChangeLogProcessor clProcessor;

    public DemoService(Gondola gondola) throws Exception {
        cluster = gondola.getClustersOnHost().get(0);

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
        if (cluster.getLocalRole() != Role.LEADER) {
            throw new NotLeaderException();
        }
        if (!entries.containsKey(key)) {
            throw new NotFoundException();
        }
        String value = entries.get(key);
        logger.info(String.format("Get key %s: %s", key, value));
        return value;
    }

    /**
     * Commits the entry to Raft log. The entries map is not updated; it is updated by the Replicator thread.
     * @param key The non-null key
     * @param value The non-null value
     */
    public void putValue(String key, String value) throws NotLeaderException {
        try {
            Command command = cluster.checkoutCommand();
            byte[] bytes = (key + ":" + value).getBytes(); // TODO implement better separator
            command.commit(bytes, 0, bytes.length);
            logger.info(String.format("Put key %s=%s", key, value));
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
     * Background thread that continuously reads committed commands from the Gondola cluster, and updates the entries
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
                    string = cluster.getCommittedCommand(appliedIndex + 1).getString();
                    appliedIndex++;
                    logger.info("Processed command {}: {}", appliedIndex, string);
                    String[] pair = string.split(":", 2);
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

    public void clearState() {
        logger.info("In demo application, no need to clear the internal storage.");
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
