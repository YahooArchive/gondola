/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.impl;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.ShardManager;
import com.yahoo.gondola.container.client.ShardManagerClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The type Direct shard manager client.
 */
public class DirectShardManagerClient implements ShardManagerClient {

    /**
     * The Shard managers.
     */
    Map<Integer, ShardManager> shardManagers = new HashMap<>();
    /**
     * The Config.
     */
    Config config;

    /**
     * Instantiates a new Direct shard manager client.
     *
     * @param config the config
     */
    public DirectShardManagerClient(Config config) {
        this.config = config;
    }

    @Override
    public boolean waitSynced(String shardId, long timeoutMs) {
        return getMemberIds(shardId)
            .parallelStream()
            .map(memberId -> getShardManager(memberId).waitSynced(shardId, timeoutMs))
            .reduce(true, (b1, b2) -> b1 && b2);
    }

    @Override
    public boolean waitApproaching(String shardId, long timeoutMs) {
        return getMemberIds(shardId)
            .parallelStream()
            .map(memberId -> getShardManager(memberId).waitApproaching(shardId, timeoutMs))
            .reduce(true, (b1, b2) -> b1 && b2);
    }

    @Override
    public void allowObserver(String shardId, String allowedShardId) {
        getMemberIds(shardId)
            .forEach(memberId -> getShardManager(memberId).allowObserver(shardId, allowedShardId));
    }

    private List<Integer> getMemberIds(String shardId) {
        return config.getMembersInShard(shardId).stream()
            .map(Config.ConfigMember::getMemberId)
            .collect(Collectors.toList());
    }

    @Override
    public void disallowObserver(String shardId, String allowedShardId) {
        getMemberIds(shardId)
            .forEach(memberId -> getShardManager(memberId).disallowObserver(shardId, allowedShardId));
    }

    @Override
    public void startObserving(String shardId, String observedShardId) {
        getMemberIds(shardId)
            .forEach(memberId -> getShardManager(memberId).startObserving(shardId, observedShardId));
    }

    @Override
    public void stopObserving(String shardId, String observedShardId) {
        getMemberIds(shardId)
            .forEach(memberId -> getShardManager(memberId).stopObserving(shardId, observedShardId));
    }

    @Override
    public void assignBucket(String shardId, Range<Integer> splitRange, String toShardId, long timeoutMs) {
        getMemberIds(shardId)
            .forEach(memberId -> getShardManager(memberId).assignBucket(shardId, splitRange, toShardId, timeoutMs));
    }

    private ShardManager getShardManager(int memberId) {
        ShardManager shardManager = shardManagers.get(memberId);
        if (shardManager == null) {
            throw new IllegalStateException("shard manager not found for memberId=" + memberId);
        }
        return shardManager;
    }

    public Map<Integer, ShardManager> getShardManagers() {
        return shardManagers;
    }
}
