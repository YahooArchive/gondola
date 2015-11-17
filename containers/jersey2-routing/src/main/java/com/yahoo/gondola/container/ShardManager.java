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
import java.util.stream.Collectors;

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


    public ShardManager(RoutingFilter filter, Config config, ShardManagerClient shardManagerClient) {
        this.filter = filter;
        this.config = config;
        this.shardManagerClient = shardManagerClient;
    }


    /**
     * Enables special mode on gondola to allow observer.
     */
    @Override
    public void allowObserver(String shardId, String allowedShardId) {
        // TODO: gondola allow observer

        logger.info("allow observer connect from shard={}", allowedShardId);
        allowedObservers.addAll(config.getMembersInShard(allowedShardId).stream().map(Config.ConfigMember::getMemberId).collect(
            Collectors.toList()));
    }


    /**
     * Disables special mode on gondola to allow observer.
     */
    @Override
    public void disallowObserver(String shardId, String allowedShardId) {
        // TODO: gondola allow observer
        logger.info("disallow observer connect from memberId={}", allowedShardId);
        allowedObservers.removeAll(config.getMembersInShard(allowedShardId).stream().map(Config.ConfigMember::getMemberId).collect(
            Collectors.toList()));
    }

    /**
     * Starts observer mode to remote cluster.
     */
    @Override
    public void startObserving(String shardId, String observedShardId) {
        // TODO: gondola start observing
        logger.info("start observer to shardId={}", observedShardId);
        observedShards.add(observedShardId);
    }


    /**
     * Stops observer mode to remote cluster, and back to normal mode.
     */
    @Override
    public void stopObserving(String shardId, String observedShardId) {
        // TODO: gondola stop observing
        logger.info("stop observer to shardId={}", observedShardId);
        observedShards.remove(observedShardId);
    }

    /**
     * Splits the bucket of fromCluster, and reassign the buckets to toCluster.
     */
    @Override
    public void assignBucket(String shardId, Range<Integer> splitRange, String toShardId, long timeoutMs) {
        MigrationType migrationType = getMigrationType(splitRange, toShardId);
        switch (migrationType) {
            case APP:
                try {
                    filter.getLockManager().blockRequestOnBuckets(splitRange);
                    filter.waitNoRequestsOnBuckets(splitRange, timeoutMs);
                    shardManagerClient.waitSynced(toShardId, timeoutMs);
                    shardManagerClient.stopObserving(toShardId, shardId);
                    filter.reassignBuckets(splitRange, toShardId, timeoutMs);
                    break;
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    // TODO: rollback
                } finally {
                    filter.getLockManager().unblockRequestOnBuckets(splitRange);
                }
                break;
            case DB:
                try {
                    shardManagerClient.waitApproaching(toShardId, -1L);
                    filter.getLockManager().blockRequest();
                    shardManagerClient.waitSynced(toShardId, timeoutMs);
                    filter.reassignBuckets(splitRange, toShardId, timeoutMs);
                    break;
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    // TODO: rollback
                } finally {
                    filter.getLockManager().unblockRequest();
                }
        }
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
