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
     * Starts the snapshot process of the storage for the specified site.
     *
     * @param siteId
     * @return the non-null location of the snapshot.
     */
    URI startSnapshot(String siteId);

    /**
     * Stops the snapshot process for the specified location.
     * @param snapshotUri
     */
    void stopSnapshot(URI snapshotUri);

    /**
     * Returns the snapshot status at the specified location.
     * @param snapshotUri
     * @return the non-null status of the snapshot.
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
