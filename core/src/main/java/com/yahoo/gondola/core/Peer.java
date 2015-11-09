/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.core;

import com.yahoo.gondola.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class represents a remote member.
 */
public class Peer {
    final static Logger logger = LoggerFactory.getLogger(Peer.class);

    // Number of outstanding backfilling messages after which the backfill thread stops
    final static int BACKFILL_AHEAD_LIMIT = 10;

    final Gondola gondola;
    final Clock clock;
    final Storage storage;
    final CoreMember cmember;
    final int peerId;

    Channel channel;

    // Send queue contains messages to be sent to remote member. It is unbounded.
    BlockingQueue<Message> outQueue = new LinkedBlockingQueue<>();

    // Index of confirmed persisted entry for this peer
    public int matchIndex;

    // These variables are used by Member to determine if it has the majority in an election
    int votedTerm = -1;
    int votedFor = -1;

    // Set to true by member when a prevote has been granted by this peer. Cleared when an ae is received.
    boolean prevoteGranted;

    // Used to synchronize the backfill variables between Member and the backfill thread
    final ReentrantLock lock = new ReentrantLock();
    final Condition backfillCond = lock.newCondition();

    // Backfill variables
    volatile boolean backfilling; // true iff nextIndex < backfillToIndex and member is the leader

    // @lock This variable is reset to 0 after receiving an AppendEntry reply from the follower
    int backfillAhead;

    // @lock This variable is set to 1 when fullSpeed is false. Otherwise it is set to BACKFILL_AHEAD_LIMIT.
    int backfillAheadLimit;

    // @lock
    int backfillToIndex;

    // @lock
    int nextIndex = -1;

    // @lock if false and backfilling, only send a single entry. Set to true when success is received.
    // Set to false when backfilling is enabled or when the nextIndex is reduced.
    boolean fullSpeed = false;

    // Stats
    Stat outMessages = new Stat();
    Stat outBytes = new Stat();
    Stat inMessages = new Stat();
    Stat inBytes = new Stat();

    // Used to check liveness of a connection
    long lastSentTs;
    long lastReceivedTs;

    CoreMember.Latency latency = new CoreMember.Latency();

    // A fake log entry representing index 0
    LogEntry entry0;

    // List of threads running in this class
    List<Thread> threads = new ArrayList<>();

    // Used to interrupt the reader thread. IO methods throw InterruptedIOException when interrupted and when closed.
    // This variable allows the reader thread to distinguish between the two. If the generation is updated, then
    // an interrupt occured.
    int generation = 0;

    // Config variables
    static boolean networkTracing;
    static boolean storageTracing;
    static int heartbeatPeriod;
    static int socketInactivityTimeout;

    public Peer(Gondola gondola, CoreMember cmember, int peerId) {
        this.gondola = gondola;
        this.clock = gondola.getClock();
        this.storage = gondola.getStorage();
        this.cmember = cmember;
        this.peerId = peerId;

        entry0 = new LogEntry(storage, 1) {
            @Override
            public void release() {
                // Don't release back into pool
            }
        };
        entry0.term = 0;
        entry0.index = 0;
        entry0.size = 0;
    }

    /**
     * Must be called before peer objects can be created.
     */
    public static void initConfig(Config config) {
        config.registerForUpdates(new Observer() {
            /*
             * Called at the time of registration and whenever the config file changes.
             */
            @Override
            public void update(Observable obs, Object arg) {
                Config config = (Config) arg;
                storageTracing = config.getBoolean("tracing.storage");
                networkTracing = config.getBoolean("tracing.network");
                heartbeatPeriod = config.getInt("raft.heartbeat_period");
                socketInactivityTimeout = config.getInt("network.channel_inactivity_timeout");
            }
        });
    }

    public void reset() {
        lock.lock();
        try {
            fullSpeed = false;
            backfilling = false;
            backfillAhead = 0;
            backfillAheadLimit = 1;
            backfillToIndex = 0;
            matchIndex = 0;
            nextIndex = -1;
            votedTerm = -1;
            votedFor = -1;
            outQueue.clear();
            lastReceivedTs = clock.now();
            prevoteGranted = false;
            backfillCond.signal();
        } finally {
            lock.unlock();
        }
    }

    public void start() {
        channel = gondola.getNetwork().createChannel(cmember.memberId, peerId);
        reset();

        // Start local threads
        assert threads.size() == 0
            : String.format("The threads have not been properly shutdown. %d threads remaining", threads.size());
        threads.add(new Receiver()); // Close the receiver before sender to avoid "writer is closed" exc from pipedinputstream
        threads.add(new Backfiller());
        threads.add(new Sender());
        threads.forEach(t -> t.start());
    }

    public void stop() {
        generation++;
        for (Thread t : threads) {
            try {
                t.interrupt();
                t.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        threads.clear();
        channel.stop(); // Call after stopping the threads, otherwise possible NPE
    }

    /******************** methods *********************/

    /**
     * Returns the channel's operational state.
     */
    public boolean isOperational() {
        return channel.isOperational();
    }

    /**
     * Sends the message to the remote member.
     * This version of send increases nextIndex and backfillToIndex.
     * Does not send if the connection to remote member is not operational or being backfilled.
     */
    public void send(Message message, int prevLogIndex) {
        assert message.getType() == Message.TYPE_APPEND_ENTRY_REQ;
        lock.lock();
        try {
            if (backfilling) {
                // Increase the backfill index to the new value
                backfillToIndex = prevLogIndex + 2;
            } else if (channel.isOperational() && (message.isHeartbeat() || nextIndex == prevLogIndex + 1)) {
                latency.head(prevLogIndex + 1);
                addOutQueue(message);

                // Increment nextIndex for each command
                nextIndex += message.numCommands();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Used to send messages that don't append to the log, such as heartbeats and request votes.
     */
    public void send(Message message) {
        if (channel.isOperational()) {
            addOutQueue(message);
        }
    }

    public int getPeerId() {
        return peerId;
    }

    /**
     * *************************** backfill ************************
     */

    class Backfiller extends Thread {
        public Backfiller() {
            setName("Backfiller-" + cmember.memberId + "-" + peerId);
            setDaemon(true);
        }

        public void run() {
            while (true) {
                try {
                    backfill();
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    // Pause to avoid a spin loop
                    try {
                        clock.sleep(10 * 1000);
                    } catch (InterruptedException e1) {
                        return;
                    }
                }
            }
        }

        /**
         * There are three stages in this method:
         * 1. Determine where to start backfilling for this peer.
         * 2. Construct a single message filled with batched commands.
         * 3. Send the message and update state.
         */
        public void backfill() throws Exception {
            MessagePool pool = gondola.getMessagePool();
            Rid rid = new Rid();
            Rid savedRid = new Rid();

            while (true) {
                // Block until the channel is operational
                channel.awaitOperational();

                // The index of the first entry to send
                int startIndex = -1;

                // Get latest savedIndex. Done outside the lock to avoid deadlock.
                cmember.saveQueue.getLatest(savedRid, true);

                // Determine where to start backfilling
                lock.lock();
                try {
                    if (backfilling && backfillAhead < backfillAheadLimit) {
                        // Make sure the storage has caught up to the next index
                        if (nextIndex > savedRid.index) {
                            logger.info("[{}-{}] Backfilling {} at index {} paused to allow storage (index={}) to catch up",
                                    gondola.getHostId(), cmember.memberId, peerId, nextIndex, savedRid.index);
                        } else {
                            startIndex = nextIndex;
                        }
                    }
                } finally {
                    lock.unlock();
                }

                // Create one backfill message with as many commands as can be batched
                int count = 0;
                Message message = null;
                if (startIndex > 0) {
                    // Get the rid for the previous entry
                    LogEntry le = getLogEntry(startIndex - 1);
                    if (le == null) {
                        throw new IllegalStateException(
                                String.format("[%s-%d] Could not retrieve index=%d to backfill %d. savedIndex=%d",
                                        gondola.getHostId(), cmember.memberId, startIndex - 1, peerId, savedRid.index));
                    }
                    rid.set(le.term, le.index);
                    le.release();

                    // Now get the info for the entry at desired index
                    le = getLogEntry(startIndex);
                    if (le == null) {
                        throw new IllegalStateException(
                                String.format("[%s-%d] Could not retrieve index=%d to backfill %d. savedIndex=%d",
                                        gondola.getHostId(), cmember.memberId, startIndex, peerId, savedRid.index));
                    }
                    
                    message = pool.checkout();
                    try {
                        message.appendEntryRequest(cmember.memberId, cmember.currentTerm, rid, cmember.commitIndex,
                                le.term, le.buffer, 0, le.size);
                        le.release();
                        count++;

                        // Batch until full
                        while (fullSpeed && startIndex + count < backfillToIndex) {
                            le = getLogEntry(startIndex + count);

                            if (le == null) {
                                // This can happen if the storage is behind
                                break;
                            } else if (le.term != rid.term || !message.canBatch(le.size)) {
                                le.release();
                                // Stop batching if the term is newer or if the message is full
                                break;
                            }

                            message.appendEntryBatch(le.buffer, 0, le.size);
                            le.release();
                            count++;
                        }
                    } catch (Exception e) {
                        message.release();
                        throw e;
                    }
                } else if (clock.now() >= lastSentTs + heartbeatPeriod) {
                    // Send a heartbeat while waiting
                    message = pool.checkout();
                    message.heartbeat(cmember.memberId, cmember.currentTerm, savedRid, cmember.commitIndex);
                    lastSentTs = clock.now(); // To avoid a spin loop
                }

                // Update variables
                lock.lock();
                try {
                    if (message != null) {
                        // Send the message if backfilling is still enabled
                        if (backfilling) {
                            addOutQueue(message);
                        }
                        message.release();
                        
                        // If nextIndex was changed while the message was being constructed,
                        // don't update nextIndex; instead, start another iteration
                        if (startIndex == nextIndex) {
                            nextIndex = startIndex + count;
                            if (nextIndex == backfillToIndex) {
                                backfilling = false;
                                logger.info("[{}-{}] Backfilling {} to {} is done",
                                        gondola.getHostId(), cmember.memberId, peerId, backfillToIndex - 1);
                            }
                        }
                        backfillAhead++;
                    }
                    if (!backfilling || backfillAhead >= backfillAheadLimit) {
                        backfillCond.await(heartbeatPeriod, TimeUnit.MILLISECONDS);
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    LogEntry getLogEntry(int index) throws Exception {
        if (index == 0) {
            return entry0;
        }
        LogEntry le = storage.getLogEntry(cmember.memberId, index);
        if (cmember.storageTracing) {
            if (le == null) {
                logger.info("[{}-{}] select(index={}) -> null",
                        gondola.getHostId(), cmember.memberId, index);
            } else {
                logger.info("[{}-{}] select(index={}) -> term={}, size={}",
                        gondola.getHostId(), cmember.memberId, index, le.term, le.size);
            }
        }
        return le;
    }

    /**
     * ****************** incoming *********************
     */

    /*
     * This thread reads messages from the socket, converts them to messages and then puts them 
     * on the receive queue.
     */
    class Receiver extends Thread {
        Receiver() {
            setName("PeerReceiver-" + cmember.memberId + "-" + peerId);
            setDaemon(true);
        }

        public void run() {
            MessageHandler handler = new MyMessageHandler();
            MessagePool pool = gondola.getMessagePool();
            Message message = pool.checkout();
            Message nextMessage = pool.checkout();
            int excess = 0;
            InputStream in = null;
            boolean errorOccurred = false;
            int generation = Peer.this.generation;

            while (true) {
                try {
                    InputStream oldIn = in;
                    in = channel.getInputStream(in, errorOccurred);
                    errorOccurred = false;
                    if (oldIn != in) {
                        // New input stream was created
                        excess = 0;
                    }

                    // Read in the rest of the message and place any excess bytes into nextMessage
                    excess = message.read(in, excess, nextMessage);
                    if (excess < 0) {
                        logger.warn("[{}-{}] recv({}): end-of-file",
                                gondola.getHostId(), cmember.memberId, peerId);
                        in = channel.getInputStream(in, true);
                        excess = 0;
                        continue;
                    }
                    lastReceivedTs = clock.now();
                    if (networkTracing) {
                        logger.info("[{}-{}] recv({}): read {} bytes",
                                gondola.getHostId(), cmember.memberId, peerId, message.size + excess);
                    }
                    gondola.getStats().incomingMessage(message.size + excess);

                    // Process message
                    message.handle(handler);
                    inMessages.value++;
                    inBytes.value += message.size;
                    message.release();

                    message = nextMessage;
                    nextMessage = pool.checkout();
                } catch (Exception e) {
                    // If this thread was interrupted, exit
                    if (generation < Peer.this.generation) {
                        nextMessage.release();
                        logger.info("peer receiver exiting: g1={} g2={}", generation, Peer.this.generation);
                        return;
                    }
                    String m = String.format("[%s-%d] Failed to receive from %d: %s",
                            gondola.getHostId(), cmember.memberId, peerId,
                            e.getMessage());
                    if ("Socket closed".equals(e.getMessage())
                            || "Read timed out".equals(e.getMessage())
                            || "Connection reset".equals(e.getMessage())) {
                        e = null; // Don't need stack trace
                    }
                    logger.warn(m, e);
                    errorOccurred = true;
                    excess = 0;
                }
            }
        }
    }

    /**
     * The peer only processes ae messages. The rest are forwarded to the member.
     */
    class MyMessageHandler extends MessageHandler {
        @Override
        public boolean appendEntryRequest(Message message, int fromMemberId, int term,
                                          int prevLogTerm, int prevLogIndex, int commitIndex, boolean isHeartbeat, int entryTerm,
                                          byte[] buffer, int bufferOffset, int bufferLen, boolean lastCommand) throws Exception {
            cmember.addIncoming(message);
            return false;
        }

        @Override
        public void appendEntryReply(Message message, int fromMemberId, int term, int mnIndex,
                                     boolean success) throws Exception {
            if (message.tracingInfo != null) {
                logger.info("[{}-{}] recv({}): {}", gondola.getHostId(), cmember.memberId, fromMemberId, message.tracingInfo);
            }

            // Cancel any ongoing prevote
            prevoteGranted = false;

            // Update the current term if necessary
            cmember.updateCurrentTerm(term, fromMemberId);

            if (cmember.isLeader()) {
                if (success) {
                    // Update the match index
                    Peer.this.matchIndex = mnIndex;
                    latency.tail(mnIndex);

                    // Advance the commit index
                    if (mnIndex > cmember.commitIndex) {
                        cmember.indexUpdated(false, false);
                    }

                    fullSpeed = true;
                    backfillAhead = BACKFILL_AHEAD_LIMIT;
                } else {
                    // Update the next index
                    setNextIndex(mnIndex, cmember.sentRid.index + 1);
                }

                // Any reply, success or fail, resets the backfill ahead count so more entries can be sent
                lock.lock();
                try {
                    if (backfilling) {
                        backfillAhead = 0;
                        backfillCond.signal();
                    }
                } finally {
                    lock.unlock();
                }
            } else {
                logger.info("[{}-{}] Ignoring ae from {} because not a leader",
                        gondola.getHostId(), cmember.memberId, fromMemberId);
            }
        }

        @Override
        public void requestVoteRequest(Message message, int fromMemberId, int term,
                                       boolean isPrevote, Rid lastRid) throws Exception {
            cmember.addIncoming(message);
        }

        @Override
        public void requestVoteReply(Message message, int fromMemberId, int term,
                                     boolean isPrevote, boolean voteGranted)
                throws Exception {
            cmember.addIncoming(message);
        }
    }

    /**
     * The nextIndex from an AppendEntryReply.
     */
    void setNextIndex(int nextIndex, int backfillToIndex) {
        assert (nextIndex <= backfillToIndex);
        lock.lock();
        try {
            // Update next index only if it's lower than the current one.
            // The reason is that sometimes the follower may send an estimate of the last index, 
            // which is higher than the true one.
            if (this.nextIndex == -1 || nextIndex < this.nextIndex || nextIndex < backfillToIndex) {
                this.nextIndex = nextIndex;
                this.backfillToIndex = backfillToIndex;

                if (!backfilling && nextIndex < backfillToIndex) {
                    backfilling = true;
                    fullSpeed = false;
                    backfillAhead = 1;
                    logger.info("[{}-{}] Backfilling {}, from {} to {}",
                            gondola.getHostId(), cmember.memberId, peerId, nextIndex, backfillToIndex - 1);
                    backfillCond.signal();
                }
            }
        } finally {
            lock.unlock();
        }

    }

    /**
     * ****************** outgoing *********************
     */

    void addOutQueue(Message message) {
        message.acquire();
        outQueue.add(message);
        outMessages.value++;
        outBytes.value += message.size;
    }

    /*
     * This thread retrieves messages from the send queue and delivers them to the remote member.
     */
    class Sender extends Thread {
        Sender() {
            setName("PeerSender-" + cmember.memberId + "-" + peerId);
            setDaemon(true);
        }

        public void run() {
            OutputStream out = null;
            boolean errorOccurred = false;

            while (true) {
                try {
                    // Check if the connection is still operational.
                    // If we don't hear from the receiver after a period, recreate the socket.
                    long now = clock.now();
                    if (now - lastSentTs > socketInactivityTimeout) {
                        // Start the timer
                        lastReceivedTs = now;
                    } else if (now - lastReceivedTs > socketInactivityTimeout) {
                        // The inactive socket may not be valid so reconnect
                        logger.info("[{}-{}] socket to {} has been inactive for {} ms, so reconnecting",
                                gondola.getHostId(), cmember.memberId, peerId, socketInactivityTimeout);
                        out = channel.getOutputStream(out, true);
                        lastReceivedTs = now;
                    }

                    out = channel.getOutputStream(out, errorOccurred);
                    errorOccurred = false;
                    Message message = outQueue.take();
                    if (message.tracingInfo != null) {
                        logger.info("[{}-{}] send({}): {}", gondola.getHostId(),
                                cmember.memberId, peerId, message.tracingInfo);
                    }
                    out.write(message.buffer, 0, message.size);
                    if (networkTracing) {
                        logger.info("[{}-{}] send({}): sent {} bytes",
                                gondola.getHostId(), cmember.memberId, peerId, message.size);
                    }
                    message.release();
                    gondola.getStats().sentMessage(message.size);
                    lastSentTs = clock.now();
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    String m = String.format("[%s-%d] Failed to send to %d: %s",
                            gondola.getHostId(), cmember.memberId, peerId,
                            e.getMessage());
                    if ("Socket closed".equals(e.getMessage())
                            || "Broken pipe".equals(e.getMessage())) {
                        e = null; // Don't need stack trace
                    }
                    logger.warn(m, e);
                    errorOccurred = true;
                }
            }
        }
    }

    // Stat
    class Stat {
        int value;
        int lastValue;
        long lastTs;

        public float getRps() {
            long now = clock.now();
            int v = value;
            float result = (v - lastValue) * 1000.0f / (now - lastTs);
            lastTs = now;
            lastValue = v;
            return result;
        }
    }
}
