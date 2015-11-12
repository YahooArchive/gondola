/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

/**
 * The Shard manager interface.
 */
public interface ShardManager {

    /**
     * Enables special mode on gondola to allow observer.
     */
    void allowObserver();

    /**
     * Disables special mode on gondola to allow observer.
     */
    void disallowObserver();

    /**
     * Starts observer mode to remote cluster.
     *
     * @param clusterId
     */
    void startObserving(String clusterId);

    /**
     * Stops observer mode to remote cluster, and back to normal mode.
     * @param clusterId
     */
    void stopObserving(String clusterId);

    /**
     * Splits the bucket of fromCluster, and reassign the buckets to toCluster.
     * @param fromCluster
     * @param toCluster
     * @param timeoutMs
     */
    void splitBucket(String fromCluster, String toCluster, long timeoutMs);

    /**
     * Merges the bucket of fromCluster to toCluster.
     *
     * @param fromCluster
     * @param toCluster
     * @param timeoutMs
     */
    void mergeBucket(String fromCluster, String toCluster, long timeoutMs);
}
