/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.core;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ZookeeperRegistration implements Registration {

    public static final String GONDOLA_DISCOVER = "/gondola/discover";
    private static final String GONDOLA_LEADER_LATCH = "/gondola/leader_latch";
    CuratorFramework client;


    // memberId -> Registry
    Map<Integer, Registry> registries;
    ObjectMapper objectMapper;
    List<RegistrationObserver> observers;
    static final Logger logger = LoggerFactory.getLogger(ZookeeperRegistration.class);
    TopologyRequirement topologyRequirements;

    public ZookeeperRegistration(CuratorFramework curatorFramework, ObjectMapper objectMapper)
        throws RegistrationException {
        this.client = curatorFramework;
        this.objectMapper = objectMapper;
        registries = new HashMap<>();
        observers = new ArrayList<>();
        ensureDiscoveryPath();
        watchChanges();
        loadRegistries();
        loadTopologyRequirement();
    }


    @Override
    public int register(String clusterId, InetAddress serverAddress, String siteId, URI serviceURI) throws
                                                                                                                       RegistrationException {
        try {
            Registry registry = new Registry();
            registry.clusterId = clusterId;
            registry.serverAddress = serverAddress;
            registry.clusterId = clusterId;
            registry.memberId = getMemberId(clusterId, serverAddress, siteId);
            // TODO: hostId is dummy, serverAddress is equal to hostId
            registry.hostId = "assigned";
            registry.siteId = siteId;

            client.create().withMode(CreateMode.EPHEMERAL)
                .forPath(GONDOLA_DISCOVER + "/" + siteId + "_" + registry.memberId,
                         objectMapper.writeValueAsBytes(registry));
            registries.put(registry.memberId, registry);
            return registry.memberId;
        } catch (Exception e) {
            throw new RegistrationException(e);
        }
    }


    @Override
    public void addObserver(RegistrationObserver observer) {
        observers.add(observer);
    }

    @Override
    public Map<Integer, Registry> getRegistries() {
        return registries;
    }

    @Override
    public void await(String clusterId, int timeoutMs) {

    }

    @Override
    public File getConfig() {
        return null;
    }

    @Override
    public List<String> getHostIds() {
        return null;
    }


    private void loadRegistries() throws RegistrationException {
        try {
            for (String nodeName : client.getChildren().forPath(GONDOLA_DISCOVER)) {
                Registry registry = objectMapper
                    .readValue(client.getData().forPath(GONDOLA_DISCOVER + "/" + nodeName), Registry.class);
                registries.put(registry.memberId, registry);
            }
        } catch (Exception e) {
            throw new RegistrationException(e);
        }
    }

    private void watchChanges() {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, GONDOLA_DISCOVER, true);
        pathChildrenCache.getListenable().addListener((curatorFramework1, pathChildrenCacheEvent) -> {
            ChildData data = pathChildrenCacheEvent.getData();
            Registry registry = objectMapper.readValue(data.getData(), Registry.class);
            switch (pathChildrenCacheEvent.getType()) {
                case CHILD_ADDED:
                case CHILD_UPDATED:
                    putRegistry(registry);
                    break;
                case CHILD_REMOVED:
                    deleteRegistry(registry);
                    break;
                case CONNECTION_RECONNECTED:
                    loadRegistries();
                    break;
                case CONNECTION_LOST:
                case CONNECTION_SUSPENDED:
                    // doing nothing
                    break;
                default:
                    throw new IllegalStateException(
                        "All event type must be implemented - " + pathChildrenCacheEvent.getType().toString());
            }
            observers.stream().forEach(r -> r.registrationUpdate(registry));
        });
        try {
            pathChildrenCache.start();
        } catch (Exception e) {
            throw new RegistrationException(e);
        }
    }

    private void putRegistry(Registry registry) {
        registries.put(registry.memberId, registry);
    }

    private void deleteRegistry(Registry registry) {
        registries.remove(registry.hostId);
    }

    private void ensureDiscoveryPath() {
        try {
            new EnsurePath(GONDOLA_DISCOVER).ensure(this.client.getZookeeperClient());
        } catch (Exception e) {
            throw new RegistrationException(e);
        }
    }

    private int getMemberId(String clusterId, InetAddress serverAddress, String siteId) {
        try {

            TopologyRequirement.Cluster cluster = topologyRequirements.clusters.get(clusterId);
            if (cluster == null) {
                throw new RegistrationException("Cluster ID - " + clusterId + " does not exist!");
            }
            Integer numNodes = cluster.numNodesOnSite.get(siteId);
            Integer siteIndex = cluster.siteIndices.get(siteId);
            int siteNumbers = cluster.numNodesOnSite.size();

            if (numNodes == null) {
                throw new RegistrationException("Site ID - " + siteId + " does not exist in " + clusterId + "!");
            }

            for (int i = 1; i <= numNodes; i++) {
                LeaderLatch
                    leaderLatch =
                    new LeaderLatch(client, GONDOLA_LEADER_LATCH + "/" + clusterId + "_" + siteId + "_" + i,
                                    serverAddress.toString());
                leaderLatch.start();
                leaderLatch.await(1, TimeUnit.SECONDS);
                if (leaderLatch.hasLeadership()) {
                    return siteIndex * siteNumbers + i;
                }
            }
            throw new RegistrationException(String.format("Cluster '%s' on site '%s' is full", clusterId, siteId));
        } catch (Exception e) {
            throw new RegistrationException(e);
        }
    }

    private void loadTopologyRequirement() {
        topologyRequirements = new TopologyRequirement();
        topologyRequirements.clusters = new HashMap<>();
        for (String clusterId : Arrays.asList("cluster1", "cluster2")) {
            TopologyRequirement.Cluster cluster = new TopologyRequirement.Cluster();
            cluster.numNodesOnSite = new HashMap<>();
            cluster.siteIndices = new HashMap<>();
            topologyRequirements.clusters.put(clusterId, cluster);
            int i = 0;
            for (String siteId : Arrays.asList("bf1", "gq1", "ne1")) {
                cluster.numNodesOnSite.put(siteId, 1);
                cluster.siteIndices.put(siteId, ++i);
            }
        }
    }

    private static class TopologyRequirement {

        Map<String, Cluster> clusters;

        static class Cluster {

            Map<String, Integer> numNodesOnSite;
            Map<String, Integer> siteIndices;
        }
    }
}
