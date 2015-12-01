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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The Direct shard manager client. Will be only used in test.
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

    Logger logger = LoggerFactory.getLogger(DirectShardManagerClient.class);
    boolean tracing = false;

    /**
     * Instantiates a new Direct shard manager client.
     *
     * @param config the config
     */
    public DirectShardManagerClient(Config config) {
        this.config = config;

        config.registerForUpdates(config1 -> tracing = config1.getBoolean("tracing.router"));
    }

    @Override
    public boolean waitSlavesSynced(String shardId, long timeoutMs) throws ShardManagerException, InterruptedException {
        trace("Waiting for slaves synced ...");
        Boolean status = getMemberIds(shardId)
            .parallelStream()
            .map(getWaitSlavesSyncedFunction(shardId, timeoutMs))
            .reduce(true, (b1, b2) -> b1 && b2);
        trace("Waiting for slaves synced {}.", status ? "success" : "failed");
        return status;
    }


    private Function<Integer, Boolean> getWaitSlavesSyncedFunction(String shardId, long timeoutMs) {
        return memberId -> {
            try {
                return getShardManager(memberId).waitSlavesSynced(shardId, timeoutMs);
            } catch (ShardManagerException|InterruptedException e) {
                return false;
            }
        };
    }

    @Override
    public boolean waitSlavesApproaching(String shardId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        trace("Waiting for slaves logs approaching...");
        Boolean status = getMemberIds(shardId)
            .parallelStream()
            .map(getWaitApproachingFunction(shardId, timeoutMs))
            .reduce(true, (b1, b2) -> b1 && b2);
        trace("Waiting for slaves logs approaching {}.", status ? "success" : "failed");
        return status;
    }

    @Override
    public void setBuckets(Range<Integer> splitRange, String fromShardId, String toShardId, boolean migrationComplete) {
        for (Config.ConfigMember m : config.getMembers()) {
            getShardManager(m.getMemberId()).setBuckets(splitRange, fromShardId, toShardId, migrationComplete);
        }
    }

    private Function<Integer, Boolean> getWaitApproachingFunction(String shardId, long timeoutMs) {
        return memberId -> {
            try {
                return getShardManager(memberId).waitSlavesApproaching(shardId, timeoutMs);
            } catch (ShardManagerException|InterruptedException e) {
                return false;
            }
        };
    }

    private List<Integer> getMemberIds(String shardId) {
        return config.getMembersInShard(shardId).stream()
            .map(Config.ConfigMember::getMemberId)
            .collect(Collectors.toList());
    }

    @Override
    public void startObserving(String shardId, String observedShardId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        for (Config.ConfigMember m : config.getMembersInShard(shardId)) {
            getShardManager(m.getMemberId()).startObserving(shardId, observedShardId, timeoutMs);
        }
    }

    @Override
    public void stopObserving(String shardId, String observedShardId, long timeoutMs) throws ShardManagerException,
                                                                                             InterruptedException {
        for (Config.ConfigMember m : config.getMembersInShard(shardId)) {
            getShardManager(m.getMemberId()).stopObserving(shardId, observedShardId, timeoutMs);
        }
    }

    @Override
    public void migrateBuckets(Range<Integer> splitRange, String fromShardId,
                               String toShardId, long timeoutMs)
        throws ShardManagerException {
        for (Config.ConfigMember m : config.getMembersInShard(fromShardId)) {
            getShardManager(m.getMemberId()).migrateBuckets(splitRange, fromShardId, toShardId, timeoutMs);
        }
        for (Config.ConfigMember m : config.getMembers()) {
            getShardManager(m.getMemberId()).setBuckets(splitRange, fromShardId, toShardId, true);
        }
    }

    public void setShardManager(int memberId, ShardManager shardManager) {
        shardManagers.put(memberId, shardManager);
    }

    private ShardManager getShardManager(int memberId) {
        ShardManager shardManager = shardManagers.get(memberId);
        if (shardManager == null) {
            throw new IllegalStateException("shard manager not found for memberId=" + memberId);
        }
        return shardManager;
    }

    private void trace(String format, Object... args) {
        if (tracing) {
            logger.info(format, args);
        }
    }

    @Override
    public void stop() {

    }
}
