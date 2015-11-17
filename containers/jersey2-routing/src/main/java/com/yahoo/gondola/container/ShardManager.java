/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.container.client.StatClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * The Shard manager
 */
class ShardManager {

    public static final int RETRY = 3;

    RoutingFilter filter;
    /**
     * The Stat client.
     */
    StatClient statClient;

    static Logger logger = LoggerFactory.getLogger(ShardManager.class);

    Set<String> observedShards = new HashSet<>();
    Set<Integer> allowedObservers = new HashSet<>();

    public ShardManager(RoutingFilter filter, StatClient statClient) {
        this.filter = filter;
        this.statClient = statClient;
    }



    /**
     * Enables special mode on gondola to allow observer.
     */
    public boolean allowObserver(int memberId) {
        // TODO: gondola allow observer
        logger.info("allow observer connect from memberId={}", memberId);
        return allowedObservers.add(memberId);
    }


    /**
     * Disables special mode on gondola to allow observer.
     */
    public boolean disallowObserver(int memberId) {
        // TODO: gondola allow observer
        logger.info("disallow observer connect from memberId={}", memberId);
        return allowedObservers.remove(memberId);
    }

    /**
     * Starts observer mode to remote cluster.
     */
    public boolean startObserving(String shardId) {
        // TODO: gondola start observing
        logger.info("start observer to shardId={}", shardId);
        return observedShards.add(shardId);
    }


    /**
     * Stops observer mode to remote cluster, and back to normal mode.
     */
    public boolean stopObserving(String shardId) {
        // TODO: gondola stop observing
        logger.info("stop observer to shardId={}", shardId);
        return observedShards.remove(shardId);
    }

    /**
     * Splits the bucket of fromCluster, and reassign the buckets to toCluster.
     */
    public void splitBucket(String fromShard, String toShard, long timeoutMs) {
        Range<Integer> splitRange = getSplitRange(fromShard);
        MigrationType migrationType = getMigrationType(fromShard, toShard);
        switch (migrationType) {
            case APP:
                for (int i = 0; i < RETRY; i++) {
                    try {
                        filter.lockManager.blockRequestOnBuckets(splitRange);
                        filter.waitNoRequestsOnBuckets(splitRange, 5000L);
                        filter.reassignBuckets(splitRange, toShard);
                        break;
                    } finally {
                        filter.lockManager.unblockRequestOnBuckets(splitRange);
                    }
                }
                break;
            case DB:
                for (int i = 0; i < RETRY; i++) {
                    try {
                        statClient.waitApproaching(toShard, -1L);
                        filter.lockManager.blockRequest();
                        statClient.waitSynced(toShard, 5000L);
                        filter.reassignBuckets(splitRange, toShard);
                    } finally {
                        filter.lockManager.unblockRequest();
                    }
                    break;
                }
        }
    }

    /**
     * Merges the bucket of fromCluster to toCluster.
     */
    public void mergeBucket(String fromShard, String toShard, long timeoutMs) {
    }

    private Range<Integer> getSplitRange(String fromShard) {
        // TODO:
        return null;
    }

    /**
     * Returns the migration type by inspect config, DB -> if two shards use different database APP -> if two shards use
     * same database.
     */
    private MigrationType getMigrationType(String fromShard, String toShard) {
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
