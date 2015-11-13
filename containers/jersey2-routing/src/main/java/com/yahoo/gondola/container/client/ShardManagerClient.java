/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.client;

import java.net.URI;

/**
 * ShardManager provides the capability to manage the shard.
 */
public interface ShardManagerClient {

    /**
     * Starts taking snapshot on host.
     *
     * @param siteId
     */
    void startSnapshot(String siteId);

    /**
     * Stops taking snapshot on host.
     * @param siteId
     */
    void stopSnapshot(String siteId);

    /**
     * Gets latest snapshot in the cluster.
     *
     * @param clusterId
     */
    URI getLatestSnapshot(String clusterId);

    /**
     * Restores specific snapshot on a host.
     * @param siteId
     * @param snapshotUri
     */
    void restoreSnapshot(String siteId, URI snapshotUri);

    /**
     * Splits cluster
     *
     * @param fromClusterId
     * @param toClusterId
     */
    void splitBucket(String fromClusterId, String toClusterId);

    /**
     * Merges cluster
     *
     * @param fromClusterId
     * @param toClusterId
     */
    void mergeBucket(String fromClusterId, String toClusterId);
}
