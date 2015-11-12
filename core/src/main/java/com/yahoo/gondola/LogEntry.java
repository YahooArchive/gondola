/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola;

/**
 * These objects are returned by the Storage class.
 */
public class LogEntry {
    public Storage storage;
    public int memberId;
    public int term;
    public int index;
    public byte[] buffer;
    public int size;

    public LogEntry(Storage storage, int capacity) {
        this.storage = storage;
        buffer = new byte[capacity];
    }

    public void copyFrom(byte[] b, int o, int len) {
        System.arraycopy(b, o, buffer, 0, len);
        size = len;
    }

    public void copyTo(byte[] b, int o) {
        System.arraycopy(buffer, 0, b, o, size);
    }

    public boolean equals(byte[] b, int o, int len) {
        if (size != len) {
            return false;
        }
        for (int i=0; i<len; i++) {
            if (b[i + o] != buffer[i]) {
                return false;
            }
        }
        return true;
    }

    /*
     * Returns this entry back into the pool.
     */
    public void release() {
        size = 0;
        storage.checkin(this);
    }

    public String toString() {
        return String.format("LogEntry(memberId=%d, term=%d, index=%d, size=%d)", memberId, term, index, size);
    }
}
