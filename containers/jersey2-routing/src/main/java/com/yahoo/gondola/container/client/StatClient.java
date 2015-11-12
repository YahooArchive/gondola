/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.client;

/**
 * The Stat client interface, get the clusters information.
 */
public interface StatClient {

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
