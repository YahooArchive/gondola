/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.yahoo.gondola.Config;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * The implementation of RegistryClient using zookeeper & apache Curator.
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
    Set<String> myHostIds = Sets.newConcurrentHashSet();

    Logger logger = LoggerFactory.getLogger(ZooKeeperSaslClient.class);

    protected ZookeeperRegistryClient(CuratorFramework curatorFramework, ObjectMapper objectMapper, Config config)
        throws IOException {
        this.client = curatorFramework;
        this.objectMapper = objectMapper;
        this.config = config;
        try {
            ensureZookeeperPath();
            watchRegistryChanges();
            loadEntries();
        } catch (Exception e) {
            throw new IOException(e);
        }
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
                Stat stat = checkFileExists(GONDOLA_HOSTS + "/" + hostId);
                if (stat == null) {
                    if (!isSleepOnce) {
                        isSleepOnce = true;
                        isComplete = false;
                        sleep(RETRY_DELAY_MS);
                        break;
                    } else {
                        return false;
                    }
                }
            }
            t = System.currentTimeMillis();
        }
        return isComplete;
    }

    private void sleep(long timeMs) {
        try {
            Thread.sleep(timeMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private Stat checkFileExists(String file) throws IOException {
        try {
            return client.checkExists().forPath(file);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private Set<String> getRelatedHostIds() {
        return myHostIds.stream()
            .map(hostId -> config.getShardIds(hostId))
            .flatMap(Collection::stream)
            .distinct()
            .map(shardId -> config.getMembersInShard(shardId))
            .flatMap(Collection::stream)
            .map(Config.ConfigMember::getHostId)
            .collect(Collectors.toSet());
    }

    private void loadEntries() throws Exception {
        for (String nodeName : client.getChildren().forPath(GONDOLA_HOSTS)) {
            Entry entry = objectMapper
                .readValue(client.getData().forPath(GONDOLA_HOSTS + "/" + nodeName), Entry.class);
            entries.put(entry.hostId, entry);
        }
    }

    private void watchRegistryChanges() throws Exception {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, GONDOLA_HOSTS, true);
        pathChildrenCache.getListenable().addListener(registryChangeHandler);
        pathChildrenCache.start();
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

    private void ensureZookeeperPath() throws Exception {
        new EnsurePath(GONDOLA_HOSTS).ensure(client.getZookeeperClient());
    }

    private String registerHostIdOnZookeeper(String siteId, InetSocketAddress serverAddress) throws Exception {
        Set<String> eligibleHostIds = getAllHostsAtSite(siteId);

        for (String key : client.getChildren().forPath(GONDOLA_HOSTS)) {
            Entry entry = objectMapper.readValue(client.getData().forPath(GONDOLA_HOSTS + "/" + key), Entry.class);
            if (entry == null) {
                continue;
            }
            if (entry.gondolaAddress.equals(serverAddress)) {
                return entry.hostId;
            }
            if (config.getSiteIdForHost(entry.hostId).equals(siteId)) {
                eligibleHostIds.remove(entry.hostId);
            }
        }

        for (String hostId : eligibleHostIds) {
            Entry entry = new Entry();
            entry.hostId = hostId;
            entry.gondolaAddress = serverAddress;
            try {
                client.create().withMode(CreateMode.EPHEMERAL)
                    .forPath(GONDOLA_HOSTS + "/" + entry.hostId, objectMapper.writeValueAsBytes(entry));
                entries.put(entry.hostId, entry);
                myHostIds.add(entry.hostId);
                return entry.hostId;
            } catch (KeeperException.NodeExistsException ignored) {
                logger.info("Failed to register for host ID {}", hostId);
            }
        }
        throw new IOException("Unable to register hostId, all hosts are full on site " + siteId);
    }

    private Set<String> getAllHostsAtSite(String siteId) throws IOException {
        Set<String> allHosts = config.getHostIds().stream()
            .filter(hostId -> siteId.equals(config.getAttributesForHost(hostId).get("siteId")))
            .collect(Collectors.toSet());
        if (allHosts.isEmpty()) {
            throw new IOException("SiteID " + siteId + " does not exist");
        }
        return allHosts;
    }
}
