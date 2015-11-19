/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.client.ShardManagerClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * The Shard manager
 */
public class ShardManager implements ShardManagerProtocol {

    Config config;
    RoutingFilter filter;

    static Logger logger = LoggerFactory.getLogger(ShardManager.class);

    private Set<String> observedShards = new HashSet<>();

    public Set<String> getObservedShards() {
        return observedShards;
    }

    public Set<Integer> getAllowedObservers() {
        return allowedObservers;
    }

    private Set<Integer> allowedObservers = new HashSet<>();

    private ShardManagerClient shardManagerClient;

    boolean tracing = false;

    public ShardManager(RoutingFilter filter, Config config, ShardManagerClient shardManagerClient) {
        this.filter = filter;
        this.config = config;
        this.shardManagerClient = shardManagerClient;
        this.config.registerForUpdates(config1 -> {
            tracing = config1.getBoolean("tracing.router");
        });
    }


    /**
     * Starts observer mode to remote cluster.
     */
    @Override
    public void startObserving(int memberId, String observedShardId) throws ShardManagerException {
        // TODO: gondola start observing
        if (tracing) {
            logger.info("[{}] Connect to shardId={} as slave", memberId, observedShardId);
        }
        observedShards.add(observedShardId);
    }


    /**
     * Stops observer mode to remote cluster, and back to normal mode.
     */
    @Override
    public void stopObserving(int memberId, String observedShardId) throws ShardManagerException {
        // TODO: gondola stop observing
        if (tracing) {
            logger.info("[{}] Disconnect to shardId={} as slave", memberId, observedShardId);
        }
        observedShards.remove(observedShardId);
    }

    /**
     * Splits the bucket of fromCluster, and reassign the buckets to toCluster.
     */
    @Override
    public void assignBucket(int memberId, Range<Integer> splitRange, String toShardId, long timeoutMs)
            throws ShardManagerException {
        String fromShardId = config.getMember(memberId).getShardId();
        if (!filter.gondola.getShard(fromShardId).getLocalMember().isLeader()) {
            return;
        }
        MigrationType migrationType = getMigrationType(splitRange, toShardId);
        switch (migrationType) {
            case APP:
                try {
                    filter.getLockManager().blockRequestOnBuckets(splitRange);
                    filter.waitNoRequestsOnBuckets(splitRange, timeoutMs);
                    shardManagerClient.waitSlavesSynced(toShardId, timeoutMs);
                    for (Config.ConfigMember member : config.getMembersInShard(toShardId)) {
                        shardManagerClient.stopObserving(member.getMemberId(), fromShardId);
                    }
                    filter.reassignBuckets(splitRange, toShardId, timeoutMs);
                    break;
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    // TODO: rollback
                    logger.warn("Error occurred, rollback!", e);
                } finally {
                    filter.getLockManager().unblockRequestOnBuckets(splitRange);
                }
                break;
            case DB:
                try {
                    shardManagerClient.waitApproaching(toShardId, -1L);
                    filter.getLockManager().blockRequest();
                    shardManagerClient.waitSlavesSynced(toShardId, timeoutMs);
                    filter.reassignBuckets(splitRange, toShardId, timeoutMs);
                    break;
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    // TODO: rollback
                } finally {
                    filter.getLockManager().unblockRequest();
                }
        }
    }

    @Override
    public boolean waitSlavesSynced(String shardId, long timeoutMs) throws ShardManagerException {
        return true;
    }

    @Override
    public boolean waitApproaching(String clusterId, long timeoutMs) throws ShardManagerException {
        return true;
    }

    /**
     * Returns the migration type by inspect config, DB -> if two shards use different database APP -> if two shards use
     * same database.
     */
    private MigrationType getMigrationType(Range<Integer> splitRange, String toShard) {
        //TODO: implement
        return MigrationType.APP;
    }

    /**
     * Two types of migration APP -> Shared the same DB DB  -> DB migration
     */
    enum MigrationType {
        APP,
        DB
    }
}
