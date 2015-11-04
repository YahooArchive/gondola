/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.container;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.gondola.Config;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
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

/**
 * The implementation of RegistryClient using zookeeper & apache Curator
 */
public class ZookeeperRegistryClient implements RegistryClient {

    public static final String GONDOLA_HOSTS = "/gondola/hosts";
    private static final long RETRY_DELAY_MS = 300;
    CuratorFramework client;


    // memberId -> Registry
    Map<String, Entry> entries = new HashMap<>();
    ObjectMapper objectMapper;
    List<Consumer<Entry>> listeners = new ArrayList<>();
    Config config;
    Set<String> myHostIds = new HashSet<>();

    protected ZookeeperRegistryClient(CuratorFramework curatorFramework, ObjectMapper objectMapper, Config config)
        throws IOException {
        this.client = curatorFramework;
        this.objectMapper = objectMapper;
        this.config = config;
        ensureZookeeperPath();
        watchRegistryChanges();
        loadEntries();
    }


    @Override
    public String register(String siteId, InetSocketAddress gondolaAddress, URI serviceUri) throws IOException {
        try {
            return registerHostIdOnZookeeper(siteId, gondolaAddress);
        } catch (Exception e) {
            throw new IOException(e);
        }
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
    public boolean waitForClusterComplete(int timeoutMs) throws IOException {
        boolean isSleepOnce = false, isComplete = false;
        long now = System.currentTimeMillis();
        long t = now;
        while (now - t < timeoutMs && !isComplete) {
            isComplete = true;
            for (String hostId : getRelatedHostIds()) {
                try {
                    Stat stat = client.checkExists().forPath(GONDOLA_HOSTS + "/" + hostId);
                    if (stat == null) {
                        if (!isSleepOnce) {
                            isSleepOnce = true;
                            isComplete = false;
                            Thread.sleep(RETRY_DELAY_MS);
                            break;
                        } else {
                            return false;
                        }
                    }
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
            t = System.currentTimeMillis();
        }
        return isComplete;
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

    private void loadEntries() throws IOException {
        try {
            for (String nodeName : client.getChildren().forPath(GONDOLA_HOSTS)) {
                Entry entry = objectMapper
                    .readValue(client.getData().forPath(GONDOLA_HOSTS + "/" + nodeName), Entry.class);
                entries.put(entry.hostId, entry);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void watchRegistryChanges() throws IOException {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, GONDOLA_HOSTS, true);
        pathChildrenCache.getListenable().addListener(registryChangeHandler);
        try {
            pathChildrenCache.start();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    PathChildrenCacheListener registryChangeHandler = (curatorFramework1, pathChildrenCacheEvent) -> {
        ChildData data = pathChildrenCacheEvent.getData();
        Entry entry = objectMapper.readValue(data.getData(), Entry.class);
        switch (pathChildrenCacheEvent.getType()) {
            case CHILD_ADDED:
            case CHILD_UPDATED:
                putRegistry(entry);
                config.setAddressForHostId(entry.hostId, entry.gondolaAddress);
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

    private void ensureZookeeperPath() throws IOException {
        try {
            new EnsurePath(GONDOLA_HOSTS).ensure(client.getZookeeperClient());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private String registerHostIdOnZookeeper(String siteId, InetSocketAddress serverAddress) throws IOException {
        try {
            Set<String> eligibleHostIds = config.getHostIds().stream()
                .map(hostId -> config.getAttributesForHost(hostId))
                .filter(a -> a.get("siteId").equals(siteId))
                .map(a -> a.get("hostId"))
                .collect(Collectors.toSet());

            if (eligibleHostIds.size() == 0) {
                throw new IOException("SiteID " + siteId + " does not exist");
            }

            for (String key : client.getChildren().forPath(GONDOLA_HOSTS)) {
                Entry entry = objectMapper.readValue(client.getData().forPath(GONDOLA_HOSTS + "/" + key), Entry.class);
                if (entry != null) {
                    if (entry.gondolaAddress.equals(serverAddress)) {
                        return entry.hostId;
                    }
                    if (config.getSiteIdForHost(entry.hostId).equals(siteId)) {
                        eligibleHostIds.remove(entry.hostId);
                    }
                }
            }

            for (String hostId : eligibleHostIds) {
                Entry entry = new Entry();
                entry.hostId = hostId;
                entry.gondolaAddress = serverAddress;
                try {
                    client.create().withMode(CreateMode.EPHEMERAL)
                        .forPath(GONDOLA_HOSTS + "/" + entry.hostId,
                                 objectMapper.writeValueAsBytes(entry));
                    entries.put(entry.hostId, entry);
                    myHostIds.add(entry.hostId);
                    return entry.hostId;
                } catch (KeeperException.NodeExistsException ignored) {}
            }
            throw new IOException("Unable to register hostId, all hosts are full on site " + siteId);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
