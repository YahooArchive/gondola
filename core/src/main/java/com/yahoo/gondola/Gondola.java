/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola;

import com.yahoo.gondola.core.CoreCmd;
import com.yahoo.gondola.core.CoreMember;
import com.yahoo.gondola.core.Message;
import com.yahoo.gondola.core.MessagePool;
import com.yahoo.gondola.core.Peer;
import com.yahoo.gondola.core.Stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Usage to commit a command:
 * Config config = Config.fromFile(confFile));
 * Gondola gondola = new Gondola(config, hostId);
 * Cluster cluster = gondola.getCluster(clusterId);
 * Command command = cluster.checkoutCommand();
 * byte[] buffer = new byte[]{0, 1, 2, 3}; // some data to commit
 * // Blocks until command has been committed
 * command.commit(buffer, 0, buffer.length);
 * <p>
 * Usage to get a committed command:
 * // Blocks until command at index 500 has been committed
 * Command command = cluster.getCommittedCommand(500);
 */
public class Gondola implements Stoppable {
    final static Logger logger = LoggerFactory.getLogger(Gondola.class);

    final Config config;
    final String hostId;
    final Stats stats = new Stats();
    final MessagePool messagePool;
    final Clock clock;
    final Network network;
    final Storage storage;

    // Get the pid of this process
    final String processId = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];


    List<Cluster> clusters = new ArrayList<Cluster>();
    Map<String, Cluster> clusterMap = new HashMap<>();
    Map<Integer, CoreMember> memberMap = new HashMap<>();

    List<Consumer<RoleChangeEvent>> listeners = new CopyOnWriteArrayList<>();

    // Change events are done by a separate thread, to avoid delaying the gondola timeouts.
    // This queue is used to deliver the change events to the thread.
    BlockingQueue<RoleChangeEvent> roleChangeEventQueue = new LinkedBlockingQueue<>();

    // List of threads running in this class
    List<Thread> threads = new ArrayList<>();

    static MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

    /**
     * @param hostId the non-null id of the current host on which to start Gondola.
     */
    public Gondola(Config config, String hostId) throws Exception {
        this.config = config;
        this.hostId = hostId;
        logger.info("------- Gondola init: {}, {}, pid={} -------", hostId, config.getAddressForHost(hostId), processId);

        // Mbean
        ObjectName objectName = new ObjectName("com.yahoo.gondola." + hostId + ":type=Stats");
        mbs.registerMBean(stats, objectName);

        // Initialize static config values
        CoreCmd.initConfig(config);
        Message.initConfig(config);
        Peer.initConfig(config);

        messagePool = new MessagePool(config, stats);

        // Create implementations
        String clockClassName = config.get(config.get("clock.impl") + ".class");
        clock = (Clock) Class.forName(clockClassName).getConstructor(Gondola.class, String.class)
                .newInstance(this, hostId);

        String networkClassName = config.get(config.get("network.impl") + ".class");
        network = (Network) Class.forName(networkClassName).getConstructor(Gondola.class, String.class)
                .newInstance(this, hostId);

        String storageClassName = config.get(config.get("storage.impl") + ".class");
        storage = (Storage) Class.forName(storageClassName).getConstructor(Gondola.class, String.class)
                .newInstance(this, hostId);

        // Create the clusters running on a host
        List<String> clusterIds = config.getClusterIds(hostId);
        for (String clusterId : clusterIds) {
            Cluster cluster = new Cluster(this, clusterId);

            clusters.add(cluster);
            clusterMap.put(clusterId, cluster);
        }
    }

    public void start() throws Exception {
        logger.info("Starting up Gondola...");
        clock.start();
        network.start();
        storage.start();
        for (Cluster c : clusters) {
            c.start();
        }

        // Start local threads
        assert threads.size() == 0;
        threads.add(new RoleChangeNotifier());
        threads.forEach(t -> t.start());
    }

    public void stop() {
        logger.info("Shutting down Gondola...");

        // Shut down all local threads
        threads.forEach(t -> t.interrupt());
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        threads.clear();

        // Shut down threads in all dependencies
        clusters.forEach(Cluster::stop);
        storage.stop();
        network.stop();
        clock.stop();
    }

    /******************** methods *********************/

    public String getHostId() {
        return hostId;
    }

    public String getProcessId() {
        return processId;
    }

    /**
     * Returns the clusters that reside on the host as specified in the constructor.
     *
     * @return non-null list of clusters.
     * @throw IllegalArgumentException if host is not found
     */
    public List<Cluster> getClustersOnHost() {
        return clusters;
    }

    public Cluster getCluster(String id) {
        return clusterMap.get(id);
    }

    Cluster get(String id) {
        return clusterMap.get(id);
    }

    public Storage getStorage() {
        return storage;
    }

    public Config getConfig() {
        return config;
    }

    public MessagePool getMessagePool() {
        return messagePool;
    }

    public Clock getClock() {
        return clock;
    }

    public Network getNetwork() {
        return network;
    }

    public Stats getStats() {
        return stats;
    }

    public void registerForRoleChanges(Consumer<RoleChangeEvent> listener) {
        listeners.add(listener);
    }

    public void unregisterForRoleChanges(Consumer<RoleChangeEvent> listener) {
        listeners.remove(listener);
    }

    public void notifyRoleChange(RoleChangeEvent evt) {
        roleChangeEventQueue.add(evt);
    }

    class RoleChangeNotifier extends Thread {
        public void run() {
            while (true) {
                try {
                    RoleChangeEvent evt = roleChangeEventQueue.take();
                    listeners.forEach(c -> c.accept(evt));
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }
}
