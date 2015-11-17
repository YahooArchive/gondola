/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.container.client.StatClient;

/**
 * The Shard manager
 */
public class ShardManager {

    public static final int RETRY = 3;

    RoutingFilter filter;
    /**
     * The Stat client.
     */
    StatClient statClient;


    public ShardManager(RoutingFilter filter, StatClient statClient) {
        this.filter = filter;
        this.statClient = statClient;
    }

    /**
     * Enables special mode on gondola to allow observer.
     */
    public void allowObserver() {
        // TODO: gondola allow observer
    }


    /**
     * Disables special mode on gondola to allow observer.
     */
    public void disallowObserver() {
        // TODO: gondola allow observer
    }

    /**
     * Starts observer mode to remote cluster.
     */
    public void startObserving(String shardId) {
        // TODO: gondola start observing
    }


    /**
     * Stops observer mode to remote cluster, and back to normal mode.
     */
    public void stopObserving(String shardId) {
        // TODO: gondola stop observing
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
