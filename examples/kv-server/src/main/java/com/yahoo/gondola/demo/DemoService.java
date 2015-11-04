/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.demo;

import com.yahoo.gondola.Cluster;
import com.yahoo.gondola.Command;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.NotLeaderException;

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

    // The map holding all the entries
    Map<String, String> entries = new ConcurrentHashMap<>();

    // Gondola cluster, used for replication
    Cluster cluster;

    Replicator replicator;

    Logger logger = LoggerFactory.getLogger(DemoService.class);

    public DemoService(Gondola gondola) throws Exception {
        cluster = gondola.getClustersOnHost().get(0);

        replicator = new Replicator();
        replicator.start();
    }

    /**
     * Get entry data, read directly from internal data structure.
     *
     * @param entryId
     * @return the value of the resource
     * @throws RecordNotFoundException
     */
    public String getValue(String entryId) throws RecordNotFoundException {
        if (!entries.containsKey(entryId)) {
            throw new RecordNotFoundException();
        }
        return entries.get(entryId);
    }

    /**
     * Commits the entry to Raft log. The entries map is not updated; it is updated by the Replicator thread.
     */
    public void putValue(String key, String value) {
        try {
            Command command = cluster.checkoutCommand();
            byte[] bytes = (key + ":" + value).getBytes();
            command.commit(bytes, 0, bytes.length);
        } catch (InterruptedException | NotLeaderException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * get appliedIndex.
     * @return applied index.
     */
    public int getAppliedIndex() {
       return replicator.getAppliedIndex();
    }

    /***
     * Used by the replicator to update internal data structure.
     */
    private void setData(String key, String value) {
        entries.put(key, value);
    }

    /**
     * Background thread that continuously reads committed commands from the Gondola cluster, and updates the entries
     * map. TODO: prevent reads until the map is fully updated.
     */
    public class Replicator extends Thread {

        int appliedIndex = 1;
        List<Consumer<String>> listeners = new ArrayList<>();

        void addListener(Consumer<String> listener) {
            listeners.add(listener);
        }

        @Override
        public void run() {
            String string;
            while (true) {
                try {
                    string = cluster.getCommittedCommand(appliedIndex).getString();
                    logger.info("Received command {} - {}", appliedIndex, string);
                    String[] pair = string.split(":", 2);
                    if (pair.length == 2) {
                        setData(pair[0], pair[1]);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    break;
                } finally {
                    appliedIndex++;
                }
            }
        }

        public int getAppliedIndex() {
            return appliedIndex;
        }
    }

    public void clearState() {
        entries.clear();
    }

    /**
     * Exception for record does not exists in the system.
     */
    public static class RecordNotFoundException extends Throwable {
    }
}
