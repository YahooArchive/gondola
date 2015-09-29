/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.rc;

import com.yahoo.gondola.LogEntry;
import com.yahoo.gondola.Storage;

/**
 * These objects are returned by the Storage class.
 */
public class RcLogEntry extends LogEntry {
    public RcLogEntry(Storage storage, int size) {
        super(storage, size);
    }

    // Don't set the size to 0.
    @Override
    public void release() {
        storage.checkin(this);
    }
}
