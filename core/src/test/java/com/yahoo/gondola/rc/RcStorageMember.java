/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.rc;

import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.LogEntry;
import com.yahoo.gondola.Storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class RcStorageMember {
    final RcStorage storage;
    final int memberId;
    int currentTerm = 1;
    int votedFor = -1;
    int maxGap = 0;
    String address = null;
    String pid = null;
    int maxIndex;
    Map<Integer, LogEntry> entries = new ConcurrentHashMap<>();

    // Items in this map are not placed into entries
    Map<Integer, Integer> pause = new ConcurrentHashMap<>();

    // All append log entry calls block on this condition, until it is allowed to store
    final ReentrantLock lock = new ReentrantLock();
    final Condition storeCond = lock.newCondition();

    RcStorageMember(RcStorage storage, int memberId) {
        this.storage = storage;
        this.memberId = memberId;
    }

    public void saveVote(int currentTerm, int votedFor) {
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
    }

    public void setMaxGap(int maxGap) {
        this.maxGap = maxGap;
    }

    public boolean hasLogEntry(int term, int index) {
        LogEntry entry = entries.get(index);
        return entry != null && entry.term == term && entry.index == index;
    }

    public LogEntry getLogEntry(int index) {
        return entries.get(index);
    }

    public LogEntry getLastLogEntry() {
        return entries.get(maxIndex);
    }

    public void appendLogEntry(int term, int index, byte[] buffer, int bufferOffset, int bufferLen) throws Exception {
        LogEntry entry = new LogEntry(storage, bufferLen) {
            @Override
            public void release() {
                // Don't release back into pool
            }
        };
        entry.copyFrom(buffer, bufferOffset, bufferLen);
        entry.term = term;
        entry.index = index;

        lock.lock();
        try {
            while (pause.containsKey(index)) {
                storeCond.await();
            }
            entries.put(index, entry);
            maxIndex = Math.max(maxIndex, index);
        } finally {
            lock.unlock();
        }
    }

    public void delete(int index) {
        entries.remove(index);

        // Recalculate max
        maxIndex = 0;
        entries.forEach((k, v) -> maxIndex = Math.max(maxIndex, k));
    }

    public int count() {
        return entries.size();
    }

    public void pauseStore(int index) {
        lock.lock();
        try {
            pause.put(index, index);
        } finally {
            lock.unlock();
        }
    }

    public void resumeStore(int index) {
        lock.lock();
        try {
            pause.remove(index);
            storeCond.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
