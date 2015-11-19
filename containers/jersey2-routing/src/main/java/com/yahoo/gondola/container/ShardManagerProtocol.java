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
     *  @param memberId         the shard id
     * @param observedShardId the observed shard id
     */
    void startObserving(int memberId, String observedShardId) throws ShardManagerException;

    /**
     * Stop observing.
     *  @param memberId         the shard id
     * @param observedShardId the observed shard id
     */
    void stopObserving(int memberId, String observedShardId) throws ShardManagerException;

    /**
     * Assign bucket.
     *  @param memberId    the shard id
     * @param splitRange the split range
     * @param toShardId  the to shard id
     * @param timeoutMs  the timeout ms
     */
    void assignBucket(int memberId, Range<Integer> splitRange, String toShardId, long timeoutMs)
        throws ShardManagerException;

    /**
     * WashardId boolean.
     *
     * @param shardId the shard id
     * @param timeoutMs the timeout ms
     * @return the boolean
     */
    boolean waitSlavesSynced(String shardId, long timeoutMs) throws ShardManagerException;

    /**
     * Wait approaching boolean.
     *
     * @param shardId the shard id
     * @param timeoutMs the timeout ms
     * @return the boolean
     */
    boolean waitApproaching (String shardId, long timeoutMs) throws ShardManagerException;

    /**
     * Thrown by shard manager methods when an error occurs.
     */
    class ShardManagerException extends Exception {
    }
}
