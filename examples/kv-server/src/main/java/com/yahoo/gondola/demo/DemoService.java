/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.demo;

import com.yahoo.gondola.Cluster;
import com.yahoo.gondola.Command;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.NotLeaderException;
import com.yahoo.gondola.container.RoutingFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DemoService {
    // The map holding all the entries
    Map<String, Entry> entries = new ConcurrentHashMap<>();

    // Gondola cluster, used for replication
    Cluster cluster;

    Logger logger = LoggerFactory.getLogger(DemoService.class);

    public DemoService() throws Exception {
        // Create the Gondola instance

        URL gondolaConfURI = DemoApplication.class.getClassLoader().getResource("gondola.conf");
        if (gondolaConfURI == null) {
            throw new FileNotFoundException("Gondola configuration not found");
        }
        File gondolaConf = new File(gondolaConfURI.getFile());
        Config config = new Config(gondolaConf);
        String hostId = System.getenv("hostId") != null ? System.getenv("hostId") : "host1";
        Gondola gondola = new Gondola(config, hostId);
        gondola.start();
        RoutingFilter.setGondola(gondola);
        cluster = gondola.getCluster("cluster1");

        new Replicator().start();
    }

    /**
     * Get entry data, read directly from internal data structure.
     */
    public Entry getEntry(String entryId){
        return entries.get(entryId);
    }

    /**
     * Commits the entry to Raft log. The entries map is not updated;
     * it is updated by the Replicator thread.
     */
    public void putEntry(String entryId, Entry entry) {
        try {
            Command command = cluster.checkoutCommand();
            byte[] bytes = (entryId + ":" + entry.getValue()).getBytes();
            command.commit(bytes, 0, bytes.length);
        } catch (InterruptedException|NotLeaderException e) {
            throw new RuntimeException(e);
        }
    }

    /***
     * Used by the replicator to update internal data structure
     */
    private void setData(String key, String value) {
        Entry entry = new Entry();
        entry.setKey(key);
        entry.setValue(value);
        entries.put(key, entry);
    }

    /**
     * Background thread that continuously reads committed commands
     * from the Gondola cluster, and updates the entries map.  
     * TODO: prevent reads until the map is fully updated.
     */
    public class Replicator extends Thread {
        int appliedIndex = 1;
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
    }
}
