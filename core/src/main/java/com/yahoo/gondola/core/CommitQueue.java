/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.core;

import com.yahoo.gondola.*;

import com.yahoo.gondola.impl.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 */
public class CommitQueue {

    final static Logger logger = LoggerFactory.getLogger(CommitQueue.class);

    final Gondola gondola;
    final CoreMember cmember;
    final Storage storage;
    final MessagePool pool;
    final Stats stats;
    final AtomicLong savedIndex;
    final AtomicLong[] matchIndices;

    // If non-null, the next add() will throw this exception
    Exception exc;

    // Lock to synchronize waiters and latestRid
    final ReentrantLock lock = new ReentrantLock();

    final Condition indexUpdated = lock.newCondition();

    // Holds all commands waiting to be committed. Must be accessed with lock.
    BlockingQueue<CoreCmd> commandQueue = new LinkedBlockingQueue<>();

    int commitIndex;

    // The highest generated rid
    Rid latestRid = new Rid();

    // List of threads running in this class
    List<Thread> threads = new ArrayList<>();

    // Holds all commands that have been assigned an index and are waiting to be committed.
    Queue<CoreCmd> waitQueue = new PriorityBlockingQueue<>(100,
            (o1, o2) -> o1.index - o2.index);

    // Holds all request for commands at an index that hasn't been committed yet.
    Queue<CoreCmd> getQueue = new PriorityBlockingQueue<>(100,
            (o1, o2) -> o1.index - o2.index);

    // Config variables
    boolean commandTracing;
    boolean storageTracing;

    CommitQueue(Gondola gondola, CoreMember cmember) throws GondolaException {
        this.gondola = gondola;
        this.cmember = cmember;
        this.storage = gondola.getStorage();
        this.pool = gondola.getMessagePool();
        this.savedIndex = null; //savedIndex;
        this.matchIndices = null; //matchIndices;
        gondola.getConfig().registerForUpdates(configListener);
        stats = gondola.getStats();
    }

    /**
     * Called at the time of registration and whenever the config file changes.
     */
    Consumer<Config> configListener = config -> {
        commandTracing = config.getBoolean("gondola.tracing.command");
        storageTracing = config.getBoolean("gondola.tracing.storage");
    };

    public void start() throws GondolaException {
        //reset();

        // Start local threads
        //threads.add(new CommandHandler());
        //threads.add(new IndexAdvancer());
        threads.forEach(t -> t.start());
    }

    public boolean stop() {
        return Utils.stopThreads(threads);
    }

    /**
     * ***************** methods ********************
     */

    /**
     * Adds command to the command queue. Does not block. Called by the command object.
     *
     * @param ccmd non-null object containing the command to be committed.
     */
    public void add(CoreCmd ccmd) {
        commandQueue.add(ccmd);
    }

    /**
     * Fetches the committed command at the specified index from storage. Blocks until the requested command is
     * committed.
     *
     * @param ccmd non-null object containing the index to be fetched.
     */
    public void get(CoreCmd ccmd, int index, int timeout)
            throws InterruptedException, TimeoutException, GondolaException {
        // Get latest saved index
        cmember.saveQueue.getLatestWait(cmember.savedRid);

        if (index > cmember.commitIndex || index > cmember.savedRid.index) {
            getQueue.add(ccmd);
            ccmd.waitForLogEntry(index, timeout);
            if (ccmd.status == Command.STATUS_TIMEOUT) {
                getQueue.remove(ccmd);
            }
        }

        // Get entry from storage
        LogEntry le = storage.getLogEntry(cmember.memberId, index);
        if (le == null) {
            throw new IllegalStateException(String.format("index=%d should be saved but is not. si=%d ci=%d",
                    index, cmember.savedRid.index, cmember.commitIndex));
        }
        if (storageTracing) {
            logger.info("[{}-{}] select(index={}) -> term={} size={}",
                    gondola.getHostId(), cmember.memberId, index, le.term, le.size);
        }

        le.copyTo(ccmd.buffer, 0);
        ccmd.size = le.size;
        ccmd.term = le.term;
        ccmd.index = index;
        ccmd.commitIndex = cmember.commitIndex;
        le.release();
        if (commandTracing) {
            logger.info("[{}-{}] got(index={}): term={} size={} status={}",
                    gondola.getHostId(), cmember.memberId, ccmd.index, ccmd.term, ccmd.size, ccmd.status);
        }
    }

    /**
     * Called by SaveQueue and Peer whenever savedIndex or matchIndex increases.
     */
    public void updateCommitIndex(int index) {
        /*
        int index = commitIndex;
        if (cmember.isFollower()) {
            saveQueue.getLatest(savedRid);
            index = Math.min(commitIndex, savedRid.index);
        }
        */
        // Signal waiters for committed commands
        CoreCmd ccmd = getQueue.peek();
        while (ccmd != null && ccmd.index <= index) {
            ccmd.update(Command.STATUS_OK, cmember.leaderId);
            getQueue.poll();
            ccmd = getQueue.peek();
        }
    }

    /**
     * This thread reads commands from the command queue and processes them. The command is assigned an index and then
     * moved to the wait queue. The command is then sent to the save queue for persistance and to the peers for
     * transmission to the remote members.
     */
    class CommandHandler extends Thread {

        CommandHandler() {
            setName("CommandHandler-" + cmember.memberId);
            setDaemon(true);
        }

        public void run() {
            while (true) {
                Message message = pool.checkout();
                try {
                    CoreCmd ccmd = commandQueue.take();
                    int prevLogIndex = latestRid.index;

                    // TODO: need to properly handle case where storage fails
                    // Prepare an append entry request for the command
                    message.appendEntryRequest(cmember.memberId, cmember.currentTerm, latestRid, commitIndex,
                            cmember.currentTerm, ccmd.buffer, 0, ccmd.size);
                    lock.lock();
                    try {
                        ccmd.term = cmember.currentTerm;
                        ccmd.index = latestRid.index + 1;
                        latestRid.set(ccmd.term, ccmd.index);
                        waitQueue.add(ccmd);

                        // Batch more commands if possible
                        ccmd = commandQueue.peek();
                        while (ccmd != null && message.canBatch(ccmd.size)) {
                            ccmd = commandQueue.remove();
                            message.appendEntryBatch(ccmd.buffer, 0, ccmd.size);
                            ccmd.term = cmember.currentTerm;
                            ccmd.index = latestRid.index + 1;
                            latestRid.set(ccmd.term, ccmd.index);
                            waitQueue.add(ccmd);
                            ccmd = commandQueue.peek();
                        }
                    } finally {
                        lock.unlock();
                    }

                    // Message is full and ready to send
                    cmember.sendAppendEntryRequest(message, prevLogIndex);
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    try {
                        // Pause to avoid a spin loop
                        Thread.sleep(1000);
                    } catch (InterruptedException e2) {
                        return;
                    }
                } finally {
                    message.release();
                }
            }
        }
    }

    /**
     *
     */
    class IndexAdvancer extends Thread {

        long[] arr = new long[matchIndices.length];

        IndexAdvancer() {
            setName("IndexAdvancer-" + cmember.memberId);
            setDaemon(true);
        }

        public void run() {
            while (true) {
                try {
                    lock.lock();
                    try {
                        indexUpdated.await();
                    } finally {
                        lock.unlock();
                    }

                    // Put all indices into an array for sorting
                    for (int i = 0; i < matchIndices.length; i++) {
                        arr[i] = matchIndices[i].get();
                    }

                    // Bubble sort in descending order
                    for (int i = 0; i < arr.length; i++) {
                        for (int j = 1; j < arr.length - i; j++) {
                            if (arr[j - 1] < arr[j]) {
                                // Swap
                                long temp = arr[j - 1];
                                arr[j - 1] = arr[j];
                                arr[j] = temp;
                            }
                        }
                    }

                    // The first index is assumed to be the locally saved index.
                    // Ensure the new commit index is not greater than the saved index.
                    int newCommitIndex = (int) Math.min(savedIndex.get(),
                            matchIndices[matchIndices.length - cmember.majority].get());

                    // Don't update with smaller commit index. This can happen if an up-to-date peer dies
                    // at the same time an out-of-date peer comes up. In this case, new writes will block until the
                    // out-of-date peer is caught up.
                    if (newCommitIndex > commitIndex) {
                        lock.lock();
                        try {
                            if (newCommitIndex > commitIndex) {
                                //cmember.sendAppendEntryRequest(message, prevLogIndex);
                                commitIndex = newCommitIndex;
                            }
                        } finally {
                            lock.unlock();
                        }
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    try {
                        // Pause to avoid a spin loop
                        Thread.sleep(1000);
                    } catch (InterruptedException e2) {
                        return;
                    }
                }
            }
        }
    }
}
