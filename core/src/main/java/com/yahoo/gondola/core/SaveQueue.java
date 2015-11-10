/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.core;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.LogEntry;
import com.yahoo.gondola.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class parallelizes the insertion of log entries into the storage system.
 * The storage system is assumed to support insertion of records with any index and this class
 * will track the latest contiguous index that raft can use to advance the commit index.
 * <p>
 * N threads are used to write to storage in parallel. When a thread is done, it compares the index
 * that it just stored with savedIndex. If it equals to savedIndex + 1, it advances savedIndex.
 * Otherwise, it will store it's index into saved.
 * <p>
 * After a thread advances savedIndex, it checks if the saved contains savedIndex + 1.
 * If so, it removes the entry from saved and advances savedIndex.
 * It continues checking and removing contiguous entries until there are no more.
 */
public class SaveQueue implements Observer {
    final static Logger logger = LoggerFactory.getLogger(SaveQueue.class);
    final Gondola gondola;
    final Storage storage;
    final Stats stats;
    final CoreMember cmember;

    // Lock to synchronize saved and savedIndex
    final ReentrantLock lock = new ReentrantLock();

    // Signaled when saved index has been initialized
    final Condition indexInitialized = lock.newCondition();

    // Set to true after initSavedIndex() is called successfully.
    boolean initialized = false;

    // @lock The highest contiguous index that has been saved.
    int savedIndex = 0;

    // @lock Contains all indexes currently being saved
    Set<Integer> saving = new HashSet<>();

    // @lock Contains indices that have been saved before the saved index.
    // It is a concurrent hashmap because various traces dump its contents outside of the lock and would get concurrent
    // modification exception otherwise. <key=index, value=term>
    Map<Integer, Integer> saved = new ConcurrentHashMap<>();

    // @lock The highest term saved so far
    int lastTerm = 0;

    // List of threads running in this class
    List<Thread> threads = new ArrayList<>();

    // The number of workers currently saving entries
    AtomicInteger busyWorkers = new AtomicInteger();

    // Contains log entries that need to be saved
    BlockingQueue<Message> workQueue = new LinkedBlockingQueue<>();

    // Used to parse and process incoming messages
    MessageHandler handler = new MyMessageHandler();

    // Holds the maximum gap that can occur between the last continguous record written and the last record written
    int maxGap;

    // Config variables
    boolean storageTracing;
    int numWorkers;

    SaveQueue(Gondola gondola, CoreMember cmember) throws Exception {
        this.gondola = gondola;
        this.cmember = cmember;
        gondola.getConfig().registerForUpdates(this);
        storage = gondola.getStorage();
        stats = gondola.getStats();
        numWorkers = gondola.getConfig().getInt("storage.save_queue_workers");

        String address = storage.getAddress(cmember.memberId);
        if (address != null && gondola.getNetwork().isActive(address)) {
            throw new IllegalStateException(String.format("[%s-%s] Process %s at address %s is currently using storage",
                    gondola.getHostId(), cmember.memberId, gondola.getProcessId(), address));
        }
        storage.setAddress(cmember.memberId, gondola.getNetwork().getAddress());
        storage.setPid(cmember.memberId, gondola.getProcessId());

        initSavedIndex();
    }

    /*
     * Called at the time of registration and whenever the config file changes.
     */
    public void update(Observable obs, Object arg) {
        Config config = (Config) arg;
        storageTracing = config.getBoolean("tracing.storage");
    }

    public void start() {
        // Create worker threads
        assert threads.size() == 0
                : String.format("The threads have not been properly shutdown. %d threads remaining", threads.size());
        for (int i = 0; i < numWorkers; i++) {
            threads.add(new Worker(i));
        }
        threads.forEach(t -> t.start());
    }

    public void stop() {
        savedIndex = 0;
        threads.forEach(t -> t.interrupt());
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        threads.clear();
    }

    /**
     * Adds the message to the queue to be saved. The savedIndex will be advanced if the
     * message is successfully stored.
     *
     * @throw Exception if an exception occurred while inserting a record into storage.
     */
    public void add(Message message) {
        message.acquire();
        workQueue.add(message);
    }

    /**
     * Atomically copies the latest saved term and index into ti.
     */
    public void getLatest(Rid rid, boolean wait) throws Exception {
        lock.lock();
        try {
            if (!initialized) {
                //cmember.indexUpdated(true, false); 
                if (wait) {
                    while (!initialized) {
                        indexInitialized.await();
                    }
                } else {
                    // The settle() method call must first succeed before this method can be used.
                    throw new IllegalStateException("The saved index has not been initialized yet");
                }
            }
            rid.term = lastTerm;
            rid.index = savedIndex;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the number of messages in the queue to be processed.
     */
    public int size() {
        return workQueue.size();
    }

    /**
     * Called whenever the Member's Raft role changes. Discards any pending operations.
     * Blocks until the operation is complete.
     *
     * @param rid is updated with the latest stored term and index
     */
    public void settle(Rid rid) throws Exception {
        logger.info("[{}-{}] Settling storage. workQ={} busy={} maxGap={}",
                gondola.getHostId(), cmember.memberId, workQueue.size(), busyWorkers.get(), maxGap);

        // Wait until the worker threads are finished. TODO: handle case where a worker is hung
        workQueue.clear();
        while (busyWorkers.get() > 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return;
            }
        }

        initSavedIndex();
        getLatest(rid, false);
    }

    /**
     * Reinitializes the savedIndex based on the entries currently in the log.
     */
    void initSavedIndex() throws Exception {
        lock.lock();
        try {
            // Don't update the saved rid in case of errors
            int newLastTerm = lastTerm;
            int newSavedIndex = savedIndex;

            // Get the latest entry, if any
            LogEntry entry = storage.getLastLogEntry(cmember.memberId);
            int lastIndex = 0;
            if (entry != null) {
                newLastTerm = entry.term;
                lastIndex = entry.index;
                entry.release();
            }

            // Quick safety check to see if another process might be sharing the same tables
            String pid = storage.getPid(cmember.memberId);
            if (!gondola.getProcessId().equals(pid)) {
                logger.warn("[{}-{}] SaveQueue: another process pid={} may be updating the same tables. Current pid={}",
                             gondola.getHostId(), cmember.memberId, pid, gondola.getProcessId());
            }

            // Get the max gap. The maxGap variable is deliberately not initialized to zero in order to 
            maxGap = storage.getMaxGap(cmember.memberId);
            logger.info("[{}-{}] Initializing save index with latest=({},{}) maxGap={}",
                    gondola.getHostId(), cmember.memberId, newLastTerm, lastIndex, maxGap);

            // Find latest contiguous index from index 1, by starting from last - maxGap
            // Move back one earlier in case the entry at last - maxGap is missing; we need to get the lastTerm
            int start = Math.max(1, lastIndex - maxGap - 1);
            for (int i = start; i <= lastIndex; i++) {
                entry = storage.getLogEntry(cmember.memberId, i);
                if (entry == null) {
                    // Found a missing entry so previous one becomes the latest
                    logger.info("[{}-{}] SaveQueue: index={} is missing (last={}). Setting savedIndex={} and deleting subsequent entries",
                            gondola.getHostId(), cmember.memberId, i, lastIndex, savedIndex);
                    deleteFrom(i + 1, lastIndex);
                    assert i > start;
                    break;
                } else {
                    newLastTerm = entry.term;
                    newSavedIndex = entry.index;
                    entry.release();
                }
            }

            // Check that there are no more gaps or extra entries
            int count = storage.count(cmember.memberId);
            if (count != newSavedIndex) {
                throw new IllegalStateException(String.format("The last index is %d but found %d entries in the log",
                        newSavedIndex, count));
            }

            // Finally update the saved rid
            lastTerm = newLastTerm;
            savedIndex = newSavedIndex;
            workQueue.clear();
            saved.clear();
            saving.clear();

            initialized = true;
            indexInitialized.signalAll();

            // All gaps removed so reset max gap
            storage.setMaxGap(cmember.memberId, 0);
            maxGap = 0;
        } finally {
            lock.unlock();
        }
    }

    // Temp for debugging
    public void verifySavedIndex() throws Exception {
        int si = savedIndex; // Capture since it might change while the last entry is being fetched
        LogEntry entry = storage.getLastLogEntry(cmember.memberId);
        if (entry != null && entry.index < si) {
            throw new IllegalStateException(String.format("Save index is not in-sync with storage: last index (%d) < save index(%d)", entry.index, si));
        }
    }

    class Worker extends Thread {
        Worker(int i) {
            setName("SaveQueueWorker-" + i);
            setDaemon(true);
            busyWorkers.incrementAndGet();
        }

        public void run() {
            Message message = null;
            while (true) {
                busyWorkers.decrementAndGet();
                try {
                    // Get a message
                    message = workQueue.take();
                } catch (InterruptedException e) {
                    break;
                }

                busyWorkers.incrementAndGet();
                try {
                    // Save the message
                    message.handle(handler);
                    message.release();
                } catch (InterruptedException e) {
                    busyWorkers.decrementAndGet();
                    break;
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    cmember.indexUpdated(true, false);
                }
            }
        }
    }

    /**
     * The only message type that needs to be handled is AppendEntry requests.
     */
    class MyMessageHandler extends MessageHandler {
        @Override
        public boolean appendEntryRequest(Message message, int fromMemberId, int term,
                                          int prevLogTerm, int prevLogIndex, int commitIndex,
                                          boolean isHeartbeat, int entryTerm, byte[] buffer, int bufferOffset, int bufferLen,
                                          boolean lastCommand) throws Exception {
            // Determine if the any entries need to be deleted
            int deletedCount = 0;
            int index = prevLogIndex + 1;
            lock.lock();
            try {
                if (saving.contains(index)) {
                    if (storageTracing) {
                        logger.info("[{}-{}] SaveQueue: index={} is currently being saved. Ignoring this request.",
                                gondola.getHostId(), cmember.memberId, index);
                    }
                    return true;
                }

                if (index <= savedIndex) {
                    // Overwriting a previous entry
                    LogEntry le = storage.getLogEntry(cmember.memberId, index);
                    if (le == null) {
                        throw new IllegalStateException(String.format("[%s] Could not retrieve index=%d. savedIndex=%d",
                                gondola.getHostId(), index, savedIndex));
                    }
                    boolean isContentsEqual = le.equals(buffer, bufferOffset, bufferLen);
                    logger.info("[{}-{}] SaveQueue: overwriting index={} which is older than the savedIndex={}. Contents are {}.",
                            gondola.getHostId(), cmember.memberId, index, savedIndex, isContentsEqual ? "identical" : "different");
                    if (isContentsEqual) {
                        // The contents haven't changed so ignore this message
                        return true;
                    } else {
                        savedIndex = index - 1;
                        logger.info("[{}-{}] SaveQueue: Setting savedIndex={} and deleting subsequent entries",
                                gondola.getHostId(), cmember.memberId, savedIndex);

                        // Determine the last entry to delete
                        int lastIndex = -1;
                        if (saving.size() > 0) {
                            lastIndex = saving.stream().max(Integer::compare).get();
                        }

                        deletedCount = deleteFrom(index, lastIndex);
                        if (deletedCount > 0) {
                            cmember.indexUpdated(false, deletedCount > 0);
                        }
                    }
                } else if (saved.containsKey(index)) {
                    // Check if the entry has already been written
                    if (storageTracing) {
                        logger.info("[{}-{}] SaveQueue: index={} has already been saved. Ignoring this request.",
                                gondola.getHostId(), cmember.memberId, index);
                    }
                    return true;
                } else {
                    // Increase maxGap if necessary.
                    int g = Math.max(maxGap, index - savedIndex);
                    if (g > maxGap) {
                        g = ((g - 1) / 10 + 1) * 10; // Round to the next higher 10
                        if (storageTracing || g % 100 == 0) {
                            logger.info("[{}-{}] SaveQueue: increasing maxGap from {} to {}",
                                    gondola.getHostId(), cmember.memberId, maxGap, g);
                        }
                        storage.setMaxGap(cmember.memberId, g);
                        maxGap = g;
                    }
                }
                saving.add(index);
            } finally {
                lock.unlock();
            }

            // Append entry outside the lock
            storage.appendLogEntry(cmember.memberId, entryTerm, index, buffer, bufferOffset, bufferLen);
            if (storageTracing) {
                logger.info("[{}-{}] insert(term={} index={} size={}) busy={} saved={} contents={}",
                        gondola.getHostId(), cmember.memberId, entryTerm, index,
                            bufferLen, busyWorkers.get(), saved.size(), new String(buffer, bufferOffset, bufferLen));
            }
            stats.savedCommand(bufferLen);

            // Update state
            int oldSavedIndex = savedIndex;
            lock.lock();
            try {
                if (!saving.remove(index)) {
                    logger.warn("[{}-{}] SaveQueue: index={} has already been removed",
                            gondola.getHostId(), cmember.memberId, index);
                }
                if (index == savedIndex + 1) {
                    // The savedIndex can be advanced immediately
                    savedIndex++;
                    index++;
                    lastTerm = entryTerm;

                    // Continue to advance savedIndex if possible
                    int start = index;
                    while (saved.containsKey(index)) {
                        lastTerm = saved.get(index);
                        saved.remove(index);
                        savedIndex++;
                        index++;
                    }
                    if (index > start && storageTracing) {
                        logger.info("[{}-{}] SaveQueue: pulled index={} to {} from saved. Remaining={}",
                                gondola.getHostId(), cmember.memberId, start, index - 1, saved.size());
                    }
                } else if (index > savedIndex) {
                    saved.put(index, entryTerm);
                } else {
                    logger.warn("[{}-{}] SaveQueue: savedIndex={} is > index={}",
                            gondola.getHostId(), cmember.memberId, savedIndex, index);
                }
            } finally {
                lock.unlock();
            }

            // Update commit index
            if (deletedCount > 0 || savedIndex > oldSavedIndex) {
                cmember.indexUpdated(false, deletedCount > 0);
            }
            return true;
        }
    }

    /**
     * Deletes entries from [index, lastIndex]. The entries are deleted backwards to avoid having to increase maxGap.
     */
    int deleteFrom(int index, int lastIndex) throws Exception {
        int deleted = 0;
        if (lastIndex < 0) {
            lastIndex = savedIndex;
            LogEntry entry = storage.getLastLogEntry(cmember.memberId);
            if (entry != null) {
                lastIndex = entry.index;
            }
        }

        for (int i = lastIndex; i >= index; i--) {
            LogEntry entry = storage.getLogEntry(cmember.memberId, i);
            if (entry != null) {
                logger.info("[{}-{}] SaveQueue: deleting index={}", gondola.getHostId(), cmember.memberId, i);
                storage.delete(cmember.memberId, i);
                deleted++;
            }
        }
        return deleted;
    }
}
