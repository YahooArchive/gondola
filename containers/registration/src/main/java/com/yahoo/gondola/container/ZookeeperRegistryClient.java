/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.container;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.deploy.association.RegisterFailedException;
import com.yahoo.gondola.Config;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ZookeeperRegistryClient implements RegistryClient {

    public static final String GONDOLA_HOSTS = "/gondola/hosts";
    CuratorFramework client;


    // memberId -> Registry
    Map<String, Entry> entries;
    ObjectMapper objectMapper;
    List<Consumer<Entry>> listeners;
    Config config;
    Set<String> myHostIds;

    protected ZookeeperRegistryClient(CuratorFramework curatorFramework, ObjectMapper objectMapper, Config config)
        throws RegistryException {
        this.client = curatorFramework;
        this.objectMapper = objectMapper;
        this.config = config;
        entries = new HashMap<>();
        listeners = new ArrayList<>();
        myHostIds = new HashSet<>();
        ensureZookeeperPath();
        watchRegistryChanges();
        loadEntries();
    }


    @Override
    public String register(String siteId, InetSocketAddress serverAddress, URI serviceURI) throws RegistryException {
        try {
            return registerHostIdOnZookeeper(siteId, serverAddress);
        } catch (Exception e) {
            throw new RegistryException(e);
        }
    }

    private List<Integer> getMemberIdsByHostId(String hostId) {
        return config.getClusterIds(hostId).stream()
            .map(clusterId -> config.getMembersInCluster(clusterId))
            .flatMap(Collection::stream)
            .filter(c -> c.getHostId().equals(hostId))
            .map(Config.ConfigMember::getMemberId)
            .distinct()
            .collect(Collectors.toList());
    }

    @Override
    public void addListener(Consumer<Entry> listener) {
        listeners.add(listener);
    }

    @Override
    public Map<String, Entry> getEntries() {
        return entries;
    }

    @Override
    public boolean await(int timeoutMs) throws RegistryException {
        boolean isSleepOnce = false;
        while (true) {
            boolean isComplete = true;
            for (String hostId : getRelatedHostIds()) {
                try {
                    Stat stat = client.checkExists().forPath(GONDOLA_HOSTS + "/" + hostId);
                    if (stat == null) {
                        try {
                            if (!isSleepOnce) {
                                isSleepOnce = true;
                                Thread.sleep(timeoutMs);
                                isComplete = false;
                                break;
                            } else {
                                return false;
                            }
                        } catch (InterruptedException ignore) {
                            return false;
                        }
                    }
                } catch (Exception e) {
                    throw new RegistryException(e);
                }
            }
            if (isComplete) {
                return true;
            }
        }
    }

    private List<String> getRelatedHostIds() {
        return myHostIds.stream()
            .map(hostId -> config.getClusterIds(hostId))
            .flatMap(Collection::stream)
            .distinct()
            .map(clusterId -> config.getMembersInCluster(clusterId))
            .flatMap(Collection::stream)
            .map(Config.ConfigMember::getHostId)
            .distinct()
            .collect(Collectors.toList());
    }

    private void loadEntries() throws RegistryException {
        try {
            for (String nodeName : client.getChildren().forPath(GONDOLA_HOSTS)) {
                Entry entry = objectMapper
                    .readValue(client.getData().forPath(GONDOLA_HOSTS + "/" + nodeName), Entry.class);
                entries.put(entry.hostId, entry);
            }
        } catch (Exception e) {
            throw new RegistryException(e);
        }
    }

    private void watchRegistryChanges() {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, GONDOLA_HOSTS, true);
        pathChildrenCache.getListenable().addListener(registryChangeHandler);
        try {
            pathChildrenCache.start();
        } catch (Exception e) {
            throw new RegistryException(e);
        }
    }

    PathChildrenCacheListener registryChangeHandler = (curatorFramework1, pathChildrenCacheEvent) -> {
        ChildData data = pathChildrenCacheEvent.getData();
        Entry entry = objectMapper.readValue(data.getData(), Entry.class);
        switch (pathChildrenCacheEvent.getType()) {
            case CHILD_ADDED:
            case CHILD_UPDATED:
                putRegistry(entry);
                config.setAddressForHostId(entry.hostId, entry.serverAddress);
                break;
            case CHILD_REMOVED:
                deleteRegistry(entry);
                config.setAddressForHostId(entry.hostId, null);
                break;
            case CONNECTION_RECONNECTED:
                loadEntries();
                break;
            case CONNECTION_LOST:
            case CONNECTION_SUSPENDED:
                // doing nothing
                break;
            default:
                throw new IllegalStateException(
                    "All event type must be implemented - " + pathChildrenCacheEvent.getType().toString());
        }
        listeners.stream().forEach(r -> r.accept(entry));
    };

    private void putRegistry(Entry entry) {
        entries.put(entry.hostId, entry);
    }

    private void deleteRegistry(Entry entry) {
        entries.remove(entry.hostId);
    }

    private void ensureZookeeperPath() {
        try {
            new EnsurePath(GONDOLA_HOSTS).ensure(this.client.getZookeeperClient());
        } catch (Exception e) {
            throw new RegistryException(e);
        }
    }

    private String registerHostIdOnZookeeper(String siteId, InetSocketAddress serverAddress) {
        try {
            List<String> eligibleHostIds = config.getHostIds().stream()
                .map(hostId -> config.getAttributesForHost(hostId))
                .filter(a -> a.get("siteId").equals(siteId))
                .map(a -> a.get("hostId"))
                .collect(Collectors.toList());

            if (eligibleHostIds.size() == 0) {
                throw new RegisterFailedException("SiteID " + siteId + " does not exist");
            }

            for (String key : client.getChildren().forPath(GONDOLA_HOSTS)) {
                Entry entry = objectMapper.readValue(client.getData().forPath(GONDOLA_HOSTS + "/" + key), Entry.class);
                if (entry != null) {
                    if (entry.serverAddress.equals(serverAddress)) {
                        return entry.hostId;
                    }
                    if (entry.siteId.equals(siteId)) {
                        eligibleHostIds.remove(entry.hostId);
                    }
                }
            }

            for (String hostId : eligibleHostIds) {
                Entry entry = new Entry();
                entry.hostId = hostId;
                entry.serverAddress = serverAddress;
                entry.siteId = siteId;
                entry.memberIds = getMemberIdsByHostId(hostId);
                entry.clusterIds = config.getClusterIds(hostId);
                client.create().withMode(CreateMode.EPHEMERAL)
                    .forPath(GONDOLA_HOSTS + "/" + entry.hostId,
                             objectMapper.writeValueAsBytes(entry));
                entries.put(entry.hostId, entry);
                myHostIds.add(entry.hostId);
                return entry.hostId;
            }
            throw new RegistryException("Unable to register hostId, all hosts are full on site " + siteId);
        } catch (Exception e) {
            throw new RegistryException(e);
        }
    }
}
