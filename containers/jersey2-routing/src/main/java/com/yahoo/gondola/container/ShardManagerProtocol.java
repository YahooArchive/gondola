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
     * Allow observer.
     *
     * @param shardId        the shard id
     * @param allowedShardId the allowed shard id
     */
    void allowObserver(String shardId, String allowedShardId);

    /**
     * Disallow observer.
     *
     * @param shardId        the shard id
     * @param allowedShardId the allowed shard id
     */
    void disallowObserver(String shardId, String allowedShardId);

    /**
     * Start observing.
     *
     * @param shardId         the shard id
     * @param observedShardId the observed shard id
     */
    void startObserving(String shardId, String observedShardId);

    /**
     * Stop observing.
     *
     * @param shardId         the shard id
     * @param observedShardId the observed shard id
     */
    void stopObserving(String shardId, String observedShardId);

    /**
     * Assign bucket.
     *
     * @param shardId    the shard id
     * @param splitRange the split range
     * @param toShardId  the to shard id
     * @param timeoutMs  the timeout ms
     */
    void assignBucket(String shardId, Range<Integer> splitRange, String toShardId, long timeoutMs);

    /**
     * Wait synced boolean.
     *
     * @param clusterid the clusterid
     * @param timeoutMs the timeout ms
     * @return the boolean
     */
    boolean waitSynced (String clusterid, long timeoutMs);

    /**
     * Wait approaching boolean.
     *
     * @param clusterId the cluster id
     * @param timeoutMs the timeout ms
     * @return the boolean
     */
    boolean waitApproaching (String clusterId, long timeoutMs);
}
