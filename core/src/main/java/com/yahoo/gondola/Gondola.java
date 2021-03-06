/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola;

import com.yahoo.gondola.core.*;

import com.yahoo.gondola.core.Utils;
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

import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Usage to commit a command:
 * <pre>
 *   Config config = Config.fromFile(confFile));
 *   Gondola gondola = new Gondola(config, hostId);
 *   gondola.start();
 *   Shard shard = gondola.getShard(shardId);
 *   Command command = shard.checkoutCommand();
 *   byte[] buffer = new byte[]{0, 1, 2, 3}; // some data to commit
 *   // Blocks until command has been committed
 *   command.commit(buffer, 0, buffer.length);
 * </pre>
 * Usage to get a committed command:
 * <pre>
 *   // Blocks until command at index 500 has been committed
 *   Command command = shard.getCommittedCommand(500);
 * </pre>
 */
public class Gondola implements Stoppable {
    final static Logger logger = LoggerFactory.getLogger(Gondola.class);

    Config config;
    String hostId;
    Stats stats = new Stats();
    MessagePool messagePool;
    Clock clock;
    Network network;
    Storage storage;

    // Get the pid of this process
    final String processId = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

    // When null, indicates that start() has not been called yet
    List<Shard> shards = new ArrayList<Shard>();

    // Shard id to shard map
    Map<String, Shard> shardMap = new HashMap<>();

    // Member id to member map
    Map<Integer, CoreMember> memberMap = new HashMap<>();

    List<Consumer<RoleChangeEvent>> listeners = new CopyOnWriteArrayList<>();

    // Change events are done by a separate thread, to avoid delaying the gondola timeouts.
    // This queue is used to deliver the change events to the thread.
    BlockingQueue<RoleChangeEvent> roleChangeEventQueue = new LinkedBlockingQueue<>();

    // List of threads running in this class
    List<Thread> threads = new ArrayList<>();

    // JMX variables
    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

    ObjectName objectName;

    // If false, init() should be called
    boolean inited;

    /**
     * @param hostId the non-null id of the current host on which to start Gondola.
     */
    public Gondola(Config config, String hostId) throws GondolaException {
        this.config = config;
        this.hostId = hostId;

        try {
            // Initialize static config values
            CoreCmd.initConfig(config);
            Message.initConfig(config);
            Peer.initConfig(config);
            ExceptionLogger.initConfig(config);

            messagePool = new MessagePool(config, stats);
            init();
        } catch (Exception e) {
            throw new GondolaException(e);
        }
    }

    private void init() throws Exception {
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

        // Create the shards running on a host
        for (String shardId : config.getShardIds(hostId)) {
            Shard shard = new Shard(this, shardId);

            shards.add(shard);
            shardMap.put(shardId, shard);
        }
        inited = true;
    }

    /**
     * Starts all threads and enables the Gondola instance.
     */
    public void start() throws GondolaException {
        try {
            logger.info("------- Gondola start: {}, {}, pid={} -------",
                    hostId, config.getAddressForHost(hostId), processId);

            if (!inited) {
                init();
            }

            // Start subsystem threads
            clock.start();
            network.start();
            storage.start();
            for (Shard s : shards) {
                s.start();
            }

            // Start local threads
            threads.add(new RoleChangeNotifier());
            threads.forEach(t -> t.start());
            objectName = new ObjectName("com.yahoo.gondola." + hostId + ":type=Stats");
            mbs.registerMBean(stats, objectName);
        } catch (Exception e) {
            throw new GondolaException(e);
        }
    }

    /**
     * Stops all threads and releases all resources. After this call, start() can be called again.
     *
     * @return true if no errors were detected.
     */
    public boolean stop() {
        logger.info("Stopping Gondola instance for host {}...", hostId);
        boolean status = network.stop();

        // Shut down all local threads
        status = Utils.stopThreads(threads) && status;
        threads.clear();

        // Shut down threads in all dependencies
        for (Shard shard : shards) {
            status = shard.stop() && status;
        }
        shards.clear();
        shardMap.clear();
        status = storage.stop() && status;
        status = clock.stop() && status;

        try {
            mbs.unregisterMBean(objectName);
            inited = false;
        } catch (InstanceNotFoundException | MBeanRegistrationException e) {
            logger.error("Unregister MBean failed", e);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return status;
    }

    /********************
     * methods
     *********************/

    public String getHostId() {
        return hostId;
    }

    public String getProcessId() {
        return processId;
    }

    /**
     * Returns the shards that appear on this host.
     * The shard information is available only after start() has been called.
     *
     * @return non-null list of shards.
     */
    public List<Shard> getShardsOnHost() {
        return shards;
    }

    /**
     * The shard information is available only after start() has been called.
     *
     * @return null if the shard id does not exist.
     */
    public Shard getShard(String id) {
        return shardMap.get(id);
    }

    public Storage getStorage() {
        return storage;
    }

    public Config getConfig() {
        return config;
    }

    /**
     * The message pool is created after start() has been called.
     */
    public MessagePool getMessagePool() {
        return messagePool;
    }

    /**
     * The clock is created after start() has been called.
     */
    public Clock getClock() {
        return clock;
    }

    /**
     * The network is created after start() has been called.
     */
    public Network getNetwork() {
        return network;
    }

    /**
     * The stats instance is created after start() has been called.
     */
    public Stats getStats() {
        return stats;
    }

    /**
     * The registered listener will be called when the role of local
     * members change.  A "local" member is a member assigned to this
     * host.  One exception is when a remote member becomes a
     * leader. In this case, the listener is called and the member
     * field will be the same as the leader field.
     *
     * Can be called before calling start(). The listener is not unregistered when stop() is called.
     */
    public void registerForRoleChanges(Consumer<RoleChangeEvent> listener) {
        listeners.add(listener);
    }

    /**
     * This method is ignored if the listener is not in the list of known listeners.
     */
    public void unregisterForRoleChanges(Consumer<RoleChangeEvent> listener) {
        listeners.remove(listener);
    }

    public void notifyRoleChange(RoleChangeEvent evt) {
        roleChangeEventQueue.add(evt);
    }

    class RoleChangeNotifier extends Thread {
        public RoleChangeNotifier() {
            setName("RoleChangeNotifier-" + hostId);
            setDaemon(true);
        }

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
