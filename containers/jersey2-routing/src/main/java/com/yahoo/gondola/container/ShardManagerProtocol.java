/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;

/**
 * The interface Shard manager protocol.
 */
public interface ShardManagerProtocol {

    /**
     * Start observing.
     *
     * @param shardId         the shard id
     * @param observedShardId the observed shard id
     * @throws ShardManagerException the shard manager exception
     */
    void startObserving(String shardId, String observedShardId) throws ShardManagerException;

    /**
     * Stop observing.
     *
     * @param shardId         the shard id
     * @param observedShardId the observed shard id
     * @throws ShardManagerException the shard manager exception
     */
    void stopObserving(String shardId, String observedShardId) throws ShardManagerException;

    /**
     * Assign bucket.
     *
     * @param splitRange the split range
     * @param toShardId  the to shard id
     * @param timeoutMs  the timeout ms
     * @throws ShardManagerException the shard manager exception
     */
    void migrateBuckets(Range<Integer> splitRange, String fromShardId, String toShardId,
                        long timeoutMs) throws ShardManagerException;

    /**
     * WashardId boolean.
     *
     * @param shardId   the shard id
     * @param timeoutMs the timeout ms
     * @return the boolean
     * @throws ShardManagerException the shard manager exception
     */
    boolean waitSlavesSynced(String shardId, long timeoutMs) throws ShardManagerException;

    /**
     * Wait approaching boolean.
     *
     * @param shardId   the shard id
     * @param timeoutMs the timeout ms
     * @return the boolean
     * @throws ShardManagerException the shard manager exception
     */
    boolean waitApproaching(String shardId, long timeoutMs) throws ShardManagerException;

    /**
     * Sets buckets.
     *
     * @param splitRange        the split range
     * @param fromShardId       the from shard id
     * @param toShardId         the to shard id
     * @param migrationComplete flag to indicate the migration is complete
     */
    void setBuckets(Range<Integer> splitRange, String fromShardId, String toShardId, boolean migrationComplete)
        throws ShardManagerException;

    /**
     * The type Shard manager exception.
     */
    class ShardManagerException extends Exception {

    }
}
