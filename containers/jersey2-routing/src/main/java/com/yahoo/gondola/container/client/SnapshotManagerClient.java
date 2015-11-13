/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.client;

import java.net.URI;

/**
 * The interface defines the protocol between snapshot controller and the snapshot taker.
 */
public interface SnapshotManagerClient {

    /**
     * Start taking snapshot on storage
     * @param siteId
     */
    URI startSnapshot(String siteId);

    /**
     * Stop taking snapshot on storage
     * @param snapshotUri
     */
    void stopSnapshot(URI snapshotUri);

    /**
     * Gets snapshot status
     * @param snapshotUri
     * @return
     */
    SnapshotStatus getSnapshotStatus(URI snapshotUri);

    /**
     *
     * @param siteId
     * @param snapshotUri
     */
    void restoreSnapshot(String siteId, URI snapshotUri);

    /**
     * The snapshot status.
     */
    enum SnapshotStatus {
        NOT_FOUND, RUNNING, ABORTED, READY
    }
}
