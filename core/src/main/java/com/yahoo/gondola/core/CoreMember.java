/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.core;

import com.yahoo.gondola.*;
import com.yahoo.gondola.Shard;

import com.yahoo.gondola.impl.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * This class is an actor inboxes - the command queue and incoming
 * messages. The command queue contains commands that need to be
 * reliably persisted. The incoming message queue contains messages
 * from the other members of this shard.
 */
public class CoreMember implements Stoppable {
    final static Logger logger = LoggerFactory.getLogger(CoreMember.class);

    final Gondola gondola;
    final Shard shard;
    final Clock clock;
    final Storage storage;
    final MessagePool pool;
    final int memberId;
    final List<Integer> peerIds;
    final boolean isPrimary;
    final SaveQueue saveQueue;
    final CommitQueue commitQueue;

    /**
     * The serialized Action execute by MainLoop.
     */
    public enum Action {
        BECOME_FOLLOWER,
        UPDATE_SAVED_INDEX,
        UPDATE_STORAGE_INDEX,
    }

    public List<Peer> peers = new ArrayList<>();
    public List<Peer> slaves = new ArrayList<>();

    // This map is used to find then peer quickly when processing incoming message
    Map<Integer, Peer> peerMap = new HashMap<>();

    // The number of members that constitute a majority for this cluster.
    // It's a convenience variable, used when calculating the commit index.
    int majority = Integer.MAX_VALUE;

    Role role = Role.FOLLOWER;

    // The identity of the current leader. -1 means the leader is not known.
    int leaderId = -1;

    // Raft variables
    int currentTerm = 1;
    int votedFor = -1;
    int commitIndex;

    // A cache of the latest (term, index) of entries that have been sent to peers.
    // In the case of a follower, holds the most recent index sent to the save queue.
    public Rid sentRid = new Rid();

    // Used to retrieve the latest stored term and index
    Rid savedRid = new Rid();

    // Used for synchronization of all the work queues.
    final ReentrantLock lock = new ReentrantLock();

    // Signaled when a command or incoming message appears in a queue.
    final Condition workAvailable = lock.newCondition();
    final Condition roleChange = lock.newCondition();

    // The wait queue holds all clients that have sent an AppendEntry and is awaiting a response
    Queue<CoreCmd> waitQueue = new PriorityBlockingQueue<CoreCmd>(100,
            (o1, o2) -> o1.index - o2.index);

    // Contains message sent from the peers. It is bounded.
    BlockingQueue<Message> incomingQueue;

    // Contains commands from clients
    //Queue<CoreCmd> commandQueue = new ConcurrentLinkedQueue<>();
    BlockingQueue<CoreCmd> commandQueue = new LinkedBlockingQueue<>();

    // Contains action requests from other threads
    Queue<Action> actionQueue = new ConcurrentLinkedQueue<>();

    // Contains time of last heartbeat. Used to determine whether a heartbeat should be sent out or not
    long lastSentTs;

    // The point in time where a follower becomes a candidate if it hasn't gotten a heartbeat before then
    long electionTimeoutTs = 0;

    // The point in time when a pre-vote should be sent out
    long prevoteTs = 0;

    // The point in time when a summary of this member's state should be logged
    long showSummaryTs = 0;

    // List of threads running in this class
    List<Thread> threads = new ArrayList<>();

    // This is declared as an instance variable to avoid having to allocate for every iteration of the main loop.
    int[] matchIndices;

    // If true, only prevotes will be processed. Is set to false when there's a prevote majority
    boolean prevotesOnly = true;

    // Used to calculate latencies
    // Leader: from time added to wait queue and release. Follower: from time receiving AE to ae
    Latency latency = new Latency();

    // Config variables
    boolean storageTracing;
    boolean commandTracing;
    int electionTimeout;
    int leaderTimeout;
    int heartbeatPeriod;
    int prevotePeriod;
    int summaryTracingPeriod;
    int incomingQueueSize;
    int waitQueueThrottleSize;
    File fileLockDir;
    FileChannel fileLockChannel;
    FileLock fileLock;
    boolean writeEmptyCommandAfterElection;

    public CoreMember(Gondola gondola, Shard shard, int memberId, List<Integer> peerIds, boolean isPrimary)
            throws Exception {
        this.gondola = gondola;
        this.shard = shard;
        this.memberId = memberId;
        this.peerIds = peerIds;
        this.isPrimary = isPrimary;
        gondola.getConfig().registerForUpdates(configListener);

        // Acquire file lock to prevent another process running with the same member id
        fileLock(true);

        clock = gondola.getClock();
        pool = gondola.getMessagePool();
        storage = gondola.getStorage();
        incomingQueue = new ArrayBlockingQueue<>(incomingQueueSize);
        saveQueue = new SaveQueue(gondola, this);
        commitQueue = new CommitQueue(gondola, this);
        gondola.getNetwork().register(memberId, channel -> acceptSlaveConnection(channel));

        for (int id : peerIds) {
            Peer peer = new Peer(gondola, this, id);
            peers.add(peer);
            peerMap.put(peer.peerId, peer);
        }

        // Initialize some convenience variables for use when calculating the commit index
        majority = (peers.size() + 1) / 2 + 1;
        matchIndices = new int[peers.size()];
        reset();
    }

    /*
     * Called at the time of registration and whenever the config file changes.
     */
    Consumer<Config> configListener = config -> {
        storageTracing = config.getBoolean("gondola.tracing.storage");
        commandTracing = config.getBoolean("gondola.tracing.command");

        heartbeatPeriod = config.getInt("raft.heartbeat_period");
        prevotePeriod = config.getInt("raft.request_vote_period");
        summaryTracingPeriod = config.getInt("gondola.tracing.summary_period");
        electionTimeout = config.getInt("raft.election_timeout");
        leaderTimeout = config.getInt("raft.leader_timeout");
        writeEmptyCommandAfterElection = config.getBoolean("raft.write_empty_command_after_election");

        incomingQueueSize = config.getInt("gondola.incoming_queue_size");
        waitQueueThrottleSize = config.getInt("gondola.wait_queue_throttle_size");
        fileLockDir = new File(config.get("gondola.file_lock_dir"));

        // Some validations
        if (heartbeatPeriod >= electionTimeout) {
            throw new IllegalStateException(String.format("heartbeat period (%d) must be < election timeout (%d)",
                    heartbeatPeriod, electionTimeout));
        }
    };

    /**
     * Reinitializes the members after changing the contents of storage.
     */
    public void reset() throws Exception {
        // Reset state variables
        becomeFollower(-1);
        lastSentTs = clock.now();
        commitIndex = 0;
        prevotesOnly = true;

        // Clear queues
        incomingQueue.clear();
        waitQueue.clear();

        // Get the latest values from storage, which has been settled via becomeX().
        currentTerm = storage.getCurrentTerm(memberId);
        votedFor = storage.getVotedFor(memberId);
        saveQueue.getLatest(savedRid, false);
        if (storageTracing) {
            logger.info("[{}] select({}): currentTerm={}, votedFor={} latest=({},{})",
                    gondola.getHostId(), memberId, currentTerm, votedFor, savedRid.term, savedRid.index);
        }
        sentRid.set(savedRid);

        // Reset peers
        if (peers != null) {
            Arrays.fill(matchIndices, 0);
            for (Peer peer : peers) {
                peer.reset();
                peer.setNextIndex(savedRid.index + 1, savedRid.index + 1);
            }
            for (Peer slave : slaves) {
                slave.reset();
                slave.setNextIndex(savedRid.index + 1, savedRid.index + 1);
            }
        }
    }

    /**
     * @throw Exception Is thrown when some thread cannot be started. The state of the instance is not known.
     */
    public void start() throws Exception {
        if (threads.size() > 0) {
            throw new IllegalStateException("start() can only be called once");
        }
        for (Peer peer : peers) {
            peer.start();
        }

        // Start local threads
        saveQueue.start();
        commitQueue.start();
        threads.add(new MainLoop());
        threads.add(new CommandHandler());
        threads.forEach(t -> t.start());
    }

    public boolean stop() {
        boolean status = true;

        for (Peer peer : peers) {
            status = peer.stop() && status;
        }
        for (Peer slave : slaves) {
            status = slave.stop() && status;
        }
        status = saveQueue.stop() && status;
        status = commitQueue.stop() && status;
        status = Utils.stopThreads(threads) && status;
        try {
            fileLock(false);
        } catch (IOException e) {
            logger.error("Could not release file lock", e);
            status = false;
        }
        return status;
    }

    /**
     * Throws an exception if the file lock can't be acquired.
     */
    void fileLock(boolean acquire) throws IOException {
        if (acquire) {
            File file = new File(fileLockDir, String.format("gondola-lock-%s-%s", gondola.getHostId(), memberId));
            fileLockChannel = new RandomAccessFile(file, "rw").getChannel();
            fileLock = fileLockChannel.tryLock();
            if (fileLock == null) {
                throw new IllegalStateException(String.format("Another process has the lock on %s", file));
            }
        } else {
            if (fileLock != null) {
                fileLock.release();
            }
        }
    }

    /**
     * ***************** methods ********************
     */

    public int getLeaderId() {
        return leaderId;
    }

    public boolean isLeader() {
        return role == Role.LEADER;
    }

    public boolean isCandidate() {
        return role == Role.CANDIDATE;
    }

    public boolean isFollower() {
        return role == Role.FOLLOWER;
    }

    /**
     * Returns whether this member is the primary member of the
     * shard. The primary member is the one that is preferred to be
     * the leader, usually for performance reasons.
     *
     * @return true if this member is the primary member
     */
    public boolean isPrimary() {
        return isPrimary;
    }

    /**
     * Forces this member to become the leader.
     */
    public void forceLeader() throws Exception {
        if (!isLeader()) {
            sendRequestVoteRequest(false);
        }
    }

    public Role getRole() {
        return role;
    }

    public int getSavedIndex() throws Exception {
        saveQueue.getLatest(savedRid, true);
        return savedRid.index;
    }

    /**
     * Adds command to the command queue. Does not block. Called by the command object.
     */
    public void addCommand(CoreCmd ccmd) {
        commandQueue.add(ccmd);
        lock.lock();
        try {
            workAvailable.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Adds message to the incoming queue. Does not block. Called by connection objects after receiving a message.
     */
    public void addIncoming(Message message) throws InterruptedException {
        message.acquire();
        if (!incomingQueue.offer(message)) {
            gondola.getStats().incomingQueueFull();
            incomingQueue.put(message);
        }

        lock.lock();
        try {
            workAvailable.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Blocks until the requested index is committed.
     *
     * @param timeout -1 means there is no timeout.
     * @param index   must be greater than 0.
     */
    public void getCommittedLogEntry(CoreCmd ccmd, int index, int timeout) throws Exception {
        commitQueue.get(ccmd, index, timeout);
    }

    /**
     * Called by save queue after advancing savedIndex or when receiving updated matchIndex from peers.
     *
     * @param storageError   If true, an error occurred during a storage operation
     * @param entriesDeleted If true, the saved index should be updated
     */
    public void indexUpdated(boolean storageError, boolean entriesDeleted) {
        if (storageError) {
            actionQueue.add(Action.BECOME_FOLLOWER);
        } else if (entriesDeleted) {
            assert role != Role.LEADER;
            actionQueue.add(Action.UPDATE_STORAGE_INDEX);
        } else {
            actionQueue.add(Action.UPDATE_SAVED_INDEX);
        }
        lock.lock();
        try {
            workAvailable.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * ***************************** main loop *********************************
     */

    class MainLoop extends Thread {

        MessageHandler handler = new MyMessageHandler();

        MainLoop() {
            setName("MainLoop-" + memberId);
            setDaemon(true);
        }

        public void run() {
            while (true) {
                try {
                    mainLoop();
                } catch (InterruptedException e) {
                    return;
                } catch (Throwable e) {
                    logger.error("Unhandled exception in MainLoop", e);
                    actionQueue.add(Action.BECOME_FOLLOWER);

                    // Pause to avoid a spin loop
                    try {
                        clock.sleep(1000);
                    } catch (InterruptedException e1) {
                        return;
                    }
                }
            }
        }

        public void mainLoop() throws Exception {
            long waitMs = 0;

            // Temporary variables to find bottleneck
            long b1 = 0;
            long b2 = 0;
            long b3 = 0;
            long b4 = 0;
            long b5 = 0;
            long bt = 0;
            long bc = 0;

            while (true) {
                long now = clock.now();
                long t1 = System.nanoTime();
                bc++;

                // Show queue information
                if (showSummary(waitMs, false)) {
                    logger.info(String.format("b1=%.1f, b2=%.1f, b3=%.1f, b4=%.1f b5=%.1f %.3fms/loop",
                            100.0 * b1 / bt, 100.0 * b2 / bt, 100.0 * b3 / bt, 100.0 * b4 / bt,
                            100.0 * b5 / bt,
                            .000001 * bt / bc));
                    b1 = b2 = b3 = b4 = b5 = bt = bc = 0;
                }

                // Process any actions
                Action action = actionQueue.poll();
                while (action != null) {
                    switch (action) {
                        case BECOME_FOLLOWER:
                            // An error occurred somewhere. Become a follower to reset state.
                            becomeFollower(-1);
                            break;
                        case UPDATE_STORAGE_INDEX:
                            // Storage had deleted some entries
                            saveQueue.getLatest(savedRid, false);
                            sentRid.set(savedRid);
                            updateWaitingCommands();
                            break;
                        case UPDATE_SAVED_INDEX:
                            // The saved index has advanced
                            if (isLeader()) {
                                advanceCommitIndex();
                            } else if (isFollower()) {
                                sendAppendEntryReply();
                            }
                            updateWaitingCommands();
                            break;
                    }
                    action = actionQueue.poll();
                }
                long t2 = System.nanoTime();
                b1 += t2 - t1;
                bt += t2 - t1;

                if (isLeader()) {
                    // Step down if not heard from a majority of peers
                    int liveCount = 1;
                    for (Peer p : peers) {
                        if (now - p.lastReceivedTs <= leaderTimeout) {
                            liveCount++;
                        }
                    }
                    if (liveCount >= majority) {
                        sendHeartbeat(false);
                    } else {
                        logger.info("[{}-{}] Leader has not heard from enough followers", gondola.getHostId(),
                                memberId);
                        becomeCandidate();
                    }
                }
                if (isCandidate()) {
                    sendPrevote();
                } else if (isFollower()) {
                    checkHeartbeat();
                }
                t1 = System.nanoTime();
                b2 += t1 - t2;
                bt += t1 - t2;

                // Process messages from peers
                Message message = incomingQueue.poll();
                while (message != null) {
                    try {
                        message.handle(handler);
                    } finally {
                        message.release();
                    }
                    message = incomingQueue.poll();
                }
                t2 = System.nanoTime();
                b3 += t2 - t1;
                bt += t2 - t1;

                // Wait for a queue to be not empty
                lock.lock();
                try {
                    if (actionQueue.peek() == null && incomingQueue.peek() == null) {
                        waitMs = computeWaitTime();
                        long t3 = System.nanoTime();
                        clock.awaitCondition(lock, workAvailable, waitMs);
                        b4 += System.nanoTime() - t3;
                    }
                } finally {
                    lock.unlock();
                }
                t1 = System.nanoTime();
                b5 += t1 - t2;
                bt += t1 - t2;
            }
        }
    }

    /**
     * ***************************** command handler *********************************
     */

    class CommandHandler extends Thread {

        public CommandHandler() {
            setName("CommandHandler-" + memberId);
            setDaemon(true);
        }

        public void run() {
            while (true) {
                try {
                    while (true) {
                        handleCommand(commandQueue.take());
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (Throwable e) {
                    logger.error(e.getMessage(), e);
                    actionQueue.add(Action.BECOME_FOLLOWER);

                    // Pause to avoid a spin loop
                    try {
                        clock.sleep(1000);
                    } catch (InterruptedException e1) {
                        return;
                    }
                }
            }
        }
    }

    // TODO: this thread is not correctly synchronized. inserts might occur just after member becomes non-leader
    void handleCommand(CoreCmd ccmd) throws Exception {
        if (isLeader()) {
            Message message = pool.checkout();
            try {
                int prevLogIndex = sentRid.index;

                // Prepare an append entry request for the command
                message.appendEntryRequest(memberId, currentTerm, sentRid, commitIndex,
                        currentTerm, ccmd.buffer, 0, ccmd.size);
                ccmd.term = currentTerm;
                ccmd.index = sentRid.index + 1;
                latency.head(ccmd.index);
                sentRid.set(ccmd.term, ccmd.index);
                waitQueue.add(ccmd);

                // Batch more commands if possible
                ccmd = commandQueue.peek();
                while (ccmd != null && message.canBatch(ccmd.size)) {
                    ccmd = commandQueue.remove();
                    message.appendEntryBatch(ccmd.buffer, 0, ccmd.size);
                    ccmd.term = currentTerm;
                    ccmd.index = sentRid.index + 1;
                    latency.head(ccmd.index);
                    sentRid.set(ccmd.term, ccmd.index);
                    waitQueue.add(ccmd);
                    ccmd = commandQueue.peek();
                }

                // Message is full and ready to send
                sendAppendEntryRequest(message, prevLogIndex);
            } finally {
                message.release();
            }
        } else {
            // Reject the request since this member is not a leader
            ccmd.update(Command.STATUS_NOT_LEADER, leaderId);
        }
    }

    /**
     * ***************************** role *********************************
     */

    void become(Role role, int leaderId) throws Exception {
        // Clear peers and reset storage state
        peers.forEach(p -> p.reset());
        slaves.forEach(p -> p.reset());
        saveQueue.settle(savedRid);
        sentRid.set(savedRid);

        Role oldRole = this.role;
        this.role = role;
        this.leaderId = leaderId;

        // Clear any waiting committers
        if (role != Role.LEADER) {
            CoreCmd ccmd = waitQueue.peek();
            while (ccmd != null) {
                ccmd.update(Command.STATUS_NOT_LEADER, leaderId);
                waitQueue.poll();
                ccmd = waitQueue.peek();
            }
        }

        // Notify gondola listeners of role change.
        if (role != oldRole) {
            gondola.notifyRoleChange(
                    new RoleChangeEvent(shard, shard.getMember(memberId), shard.getMember(leaderId),
                            oldRole, role));
        }
    }

    public void becomeLeader() throws Exception {
        logger.info("[{}-{}] Becomes LEADER for term {} {}",
                gondola.getHostId(), memberId, currentTerm, isPrimary ? "(primary)" : "");
        become(Role.LEADER, memberId);

        // Initialize raft variables
        for (Peer peer : peers) {
            int nextIndex = sentRid.index + 1;
            peer.setNextIndex(nextIndex, nextIndex);
            peer.lastReceivedTs = clock.now();
        }
        for (Peer slave : slaves) {
            int nextIndex = sentRid.index + 1;
            slave.setNextIndex(nextIndex, nextIndex);
            slave.lastReceivedTs = clock.now();
        }

        // If command queue is empty, add a no-op command to commit entries from the previous term
        if (writeEmptyCommandAfterElection
                && commandQueue.size() == 0
                && sentRid.term > 0
                && sentRid.term < currentTerm) {
            commandQueue.add(new CoreCmd(gondola, shard, this));
        }
    }

    public void becomeCandidate() throws Exception {
        logger.info("[{}-{}] Becomes CANDIDATE {}",
                gondola.getHostId(), memberId, isPrimary ? "(primary)" : "");
        shutdownSlaves();
        become(Role.CANDIDATE, -1);

        // Set time to send prevote
        prevoteTs = clock.now() + (long) ((Math.random() * prevotePeriod));
    }

    public void becomeFollower(int leaderId) throws Exception {
        logger.info("[{}-{}] Becomes FOLLOWER of {} {}",
                    gondola.getHostId(), memberId, leaderId, isPrimary ? "(primary)" : "");
        shutdownSlaves();
        become(Role.FOLLOWER, leaderId);

        // To avoid the case where this member becomes a candidate and an RV is received for the current term
        if (votedFor == -1 && leaderId != -1) {
            storage.saveVote(memberId, currentTerm, leaderId);
        }

        // Set timeout
        electionTimeoutTs = clock.now() + electionTimeout;
    }

    /**
     * ***************************** work *********************************
     */

    /**
     * Called as a result of incoming append entry reply messages. Attempts to release as many waiting commands as
     * possible.
     */
    void updateWaitingCommands() throws Exception {
        int index = commitIndex;
        if (isLeader()) {
            // Commit
            CoreCmd ccmd = waitQueue.peek();
            while (ccmd != null && ccmd.index <= index) {
                latency.tail(ccmd.index);
                ccmd.update(Command.STATUS_OK, leaderId);
                waitQueue.poll();
                ccmd = waitQueue.peek();
            }
        }
        // Update the getters
        saveQueue.getLatest(savedRid, false);
        index = Math.min(commitIndex, savedRid.index); // The min is needed for followers
        commitQueue.updateCommitIndex(index);
    }

    void sendHeartbeat(boolean force) {
        // Send heartbeat only if there were no sent messages within the period
        if (force || clock.now() >= lastSentTs + heartbeatPeriod) {
            sendHeartbeatRequest(sentRid);
        }
    }

    /**
     * Sends a prevote if it's time to send one.
     */
    void sendPrevote() throws Exception {
        if (clock.now() >= prevoteTs) {
            // Clear the prevotes before sending prevote
            peers.forEach(p -> p.prevoteGranted = false);
            slaves.forEach(p -> p.prevoteGranted = false);
            sendRequestVoteRequest(true);
        }
    }

    void save(int term, int votedFor) throws Exception {
        storage.saveVote(memberId, term, votedFor);
        if (storageTracing) {
            logger.info("[{}-{}] update(term={} votedFor={})",
                    gondola.getHostId(), memberId, term, votedFor);
        }
    }

    void checkHeartbeat() throws Exception {
        long late = clock.now() - electionTimeoutTs;
        if (late >= 0) {
            logger.info("[{}-{}] No heartbeat from {} in {}ms (timeout={}ms)",
                    gondola.getHostId(), memberId, leaderId, electionTimeout + late, electionTimeout);
            becomeCandidate();
        }
    }

    long computeWaitTime() {
        long now = clock.now();
        long t = 0;
        if (isLeader()) {
            t = heartbeatPeriod - (now - lastSentTs);
        } else if (isCandidate()) {
            t = Math.max(0, prevoteTs - now);
        } else {
            t = Math.min(electionTimeoutTs - now, heartbeatPeriod);
        }
        return t;
    }

    /**
     * If the term is > currentTerm, update currentTerm and become a follower.
     */
    void updateCurrentTerm(int term, int fromMemberId) throws Exception {
        if (term > currentTerm) {
            int oldCterm = currentTerm;
            currentTerm = term;
            votedFor = -1;

            // Persist the new current term and voted for
            save(currentTerm, votedFor);

            // Rule for all roles
            if (!isFollower()) {
                becomeFollower(-1);
                logger.info("[{}-{}] Became a follower because term {} from {} is > currentTerm {}",
                        gondola.getHostId(), memberId, term, fromMemberId, oldCterm);
            }
        }
    }

    /**
     * Computes the commit index based on the save index and match indices from all the peers.
     * TODO: do not advance unless the latest commited entry's term matches the current term.
     */
    void advanceCommitIndex() throws Exception {
        for (int i = 0; i < peers.size(); i++) {
            Peer peer = peers.get(i);
            matchIndices[i] = peer.matchIndex;
        }

        // Bubble sort in descending order
        for (int i = 0; i < matchIndices.length; i++) {
            for (int j = 1; j < matchIndices.length - i; j++) {
                if (matchIndices[j - 1] < matchIndices[j]) {
                    // Swap
                    int temp = matchIndices[j - 1];
                    matchIndices[j - 1] = matchIndices[j];
                    matchIndices[j] = temp;
                }
            }
        }

        // New commit index cannot be > locally saved index
        saveQueue.getLatest(savedRid, false);
        int newCommitIndex = Math.min(savedRid.index, matchIndices[matchIndices.length - majority]);

        // Don't update with smaller commit index. This can happen if an up-to-date peer dies
        // at the same time an out-of-date peer comes up. In this case, new writes will block until the
        // out-of-date peer is caught up.
        if (newCommitIndex > commitIndex) {
            commitIndex = newCommitIndex;
        }
    }

    public boolean showSummary(long waitMs, boolean force) {
        // Show queue information
        long now = clock.now();
        if (force || now > showSummaryTs) {
            Stats stats = gondola.getStats();
            logger.info(
                    String.format("[%s-%d] %s pid=%s wait=%dms cmdQ=%d waitQ=%d in=%d"
                                    + "|%.1f/s out=%.1f/s lat=%.3fms/%.3fms",
                            gondola.getHostId(), memberId, role == Role.FOLLOWER && masterId >= 0 ? "SLAVE" : role,
                            gondola.getProcessId(), waitMs, commandQueue.size(), waitQueue.size(),
                            incomingQueue.size(), stats.incomingMessagesRps, stats.sentMessagesRps,
                            CoreCmd.commitLatency.get(), latency.get()));
            logger.info(String.format("[%s-%d] - leader=%d cterm=%d ci=%d latest=(%d,%d) votedFor=%d msgPool=%d/%d",
                    gondola.getHostId(), memberId, leaderId, currentTerm, commitIndex,
                    sentRid.term, sentRid.index, votedFor,
                    pool.size(), pool.createdCount));
            logger.info(String.format("[%s-%d] - storage %.1f/s ti=(%d,%d) saveQ=%d gap=%d done=%d",
                    gondola.getHostId(), memberId,
                    stats.savedCommandsRps, saveQueue.lastTerm, saveQueue.savedIndex,
                    saveQueue.workQueue.size(), saveQueue.maxGap, saveQueue.saved.size()));
            for (Peer peer : peers) {
                logger.info(peerInfo(peer, false));
            }
            for (Peer slave : slaves) {
                logger.info(peerInfo(slave, true));
            }

            if (commandTracing) {
                for (CoreCmd c : waitQueue) {
                    logger.info("[{}-{}] commit waiter: term={} index={}",
                            gondola.getHostId(), memberId, c.term, c.index);
                }
                for (CoreCmd c : commitQueue.getQueue) {
                    logger.info("[{}-{}] get waiter: index={}",
                            gondola.getHostId(), memberId, c.index);
                }
            }
            showSummaryTs = now + summaryTracingPeriod;
            return true;
        }
        return false;
    }

    private String peerInfo(Peer peer, boolean isSlave) {
        return String.format(
                "[%s-%d] - %s=%d %s in=%.1f/s|%.1fB/s out=%d|"
                        + "%.1f/s|%.1fB/s ni=%d mi=%d vf=(%d,%d)%s%s lat=%.3fms",
                gondola.getHostId(), memberId, isSlave ? "slave" : "peer",
                peer.peerId, peer.isOperational() ? "U" : "D",
                peer.inMessages.getRps(), peer.inBytes.getRps(),
                peer.outQueue.size(), peer.outMessages.getRps(), peer.outBytes.getRps(),
                peer.nextIndex, peer.matchIndex, peer.votedTerm, peer.votedFor,
                peer.prevoteGranted ? " prevote" : "",
                peer.backfilling ? String.format(" bf=%d, bfa=%d", peer.backfillToIndex,
                        peer.backfillAhead) : "",
                peer.latency.get());
    }

    /****************************** outgoing messages *************************/

    /**
     * @param rid non-null object containing the leader's latest term and index.
     */
    public void sendHeartbeatRequest(Rid rid) {
        Message message = pool.checkout();
        try {
            message.heartbeat(memberId, currentTerm, rid, commitIndex);

            peers.forEach(p -> p.send(message, rid.index - 1));
            slaves.forEach(p -> p.send(message, rid.index - 1));
        } finally {
            message.release();
        }
        lastSentTs = clock.now();
    }

    /**
     * @param message      A non-null append entry request message.
     * @param prevLogIndex This value is identical to the prevLogIndex in message.
     */
    public void sendAppendEntryRequest(Message message, int prevLogIndex) throws Exception {
        // Send command first
        assert message.getType() == Message.TYPE_APPEND_ENTRY_REQ;
        peers.forEach(p -> p.send(message, prevLogIndex));
        slaves.forEach(p -> p.send(message, prevLogIndex));

        // Then store command
        saveQueue.add(message);

        lastSentTs = clock.now();
    }

    /**
     * Sent after follower has advanced the savedIndex.
     */
    public void sendAppendEntryReply() throws Exception {
        Peer leader = peerMap.get(leaderId);
        if (leader != null) {
            Message message = pool.checkout();
            try {
                saveQueue.getLatest(savedRid, false);
                message.appendEntryReply(memberId, currentTerm, savedRid.index, true, false);
                leader.send(message);
                latency.tail(savedRid.index);
            } finally {
                message.release();
            }
            lastSentTs = clock.now();
        }
    }

    /**
     * If not a prevote, increments the current term before sending out the message.
     */
    public void sendRequestVoteRequest(boolean isPrevote) throws Exception {
        if (!isPrevote) {
            // Increment the current term
            currentTerm++;
            votedFor = -1;

            // Attempt to store the update before sending out the message, in case storage is not working
            // If the storage is down, no request is sent out
            save(currentTerm, memberId);
            votedFor = memberId;
        }
        prevotesOnly = isPrevote;

        Message message = pool.checkout();
        try {
            // Create and send the message to all the peers
            saveQueue.getLatest(savedRid, false);
            message.requestVoteRequest(memberId, currentTerm, isPrevote, savedRid);
            peers.forEach(p -> p.send(message));
        } finally {
            message.release();
        }

        // Set the time to send the next prevote in case there's no reply
        lastSentTs = clock.now();
        prevoteTs = lastSentTs + (long) ((Math.random() * prevotePeriod));

        // In the case of a request vote, give the peers the maximum time to respond before sending the next prevote
        if (!isPrevote) {
            prevoteTs += electionTimeout;
        }
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    /**
     * ***************************** incoming messages *********************************
     */

    // If not max_value, indicates that the last append entry message does not match the entry in the current log.
    int failedNextIndex = Integer.MAX_VALUE;

    class MyMessageHandler extends MessageHandler {

        @Override
        public boolean appendEntryRequest(Message message, int fromMemberId, int term,
                                          int prevLogTerm, int prevLogIndex, int commitIndex, boolean isHeartbeat,
                                          int entryTerm,
                                          byte[] buffer, int bufferOffset, int bufferLen, boolean lastCommand)
                throws Exception {
            latency.head(prevLogIndex + 1);
            electionTimeoutTs = clock.now() + electionTimeout;
            if (message.tracingInfo != null) {
                logger.info("[{}-{}] recv({}): {}", gondola.getHostId(), memberId, fromMemberId, message.tracingInfo);
            }
            Peer peer = peerMap.get(fromMemberId);
            if (peer == null) {
                logger.error("Received AE from unknown member {}", fromMemberId);
                return false;
            }

            // Reject the request
            if (term < currentTerm) {
                message.appendEntryReply(memberId, currentTerm, prevLogIndex + 1, false, false);
                peer.send(message);
                return false;
            }

            // Become a follower if the sender's term is later
            updateCurrentTerm(term, fromMemberId);

            // Get latest saved state
            saveQueue.getLatest(savedRid, false);

            if (!isFollower()) {
                // If sender's log is up-to-date, follow the sender
                boolean validLog = prevLogTerm > savedRid.term
                        || (prevLogTerm == savedRid.term && prevLogIndex >= savedRid.index);
                if (validLog) {
                    becomeFollower(fromMemberId);
                }
            }

            if (isFollower()) {
                // If prevLogIndex is 0, always accept the request
                if (prevLogIndex > 0 && !sentRid.equals(prevLogTerm, prevLogIndex)) {
                    // This is an newer entry.
                    // Check if the previous rid in the message is in storage.
                    // As an optimization, if the index is > the failedNextIndex, there's no point checking.
                    // boolean hasEntry = prevLogIndex < failedNextIndex &&
                    //                                   storage.hasLogEntry(memberId, prevLogTerm, prevLogIndex);
                    boolean hasEntry = storage.hasLogEntry(memberId, prevLogTerm, prevLogIndex);
                    if (storageTracing) {
                        logger.info("[{}-{}] hasEntry(term={} index={}) -> {}",
                                gondola.getHostId(), memberId, prevLogTerm, prevLogIndex, hasEntry);
                    }

                    // If storage doesn't have this entry, reject the request and include the last saved index
                    if (!hasEntry) {
                        saveQueue.verifySavedIndex();
                        // Reject the request and include the last saved index or the previous index,
                        // whichever is smaller
                        failedNextIndex = Math.min(failedNextIndex, Math.min(prevLogIndex, savedRid.index + 1));
                        message.appendEntryReply(memberId, currentTerm, failedNextIndex, false, false);
                        peer.send(message);
                        return false;
                    }
                }

                // Reset the failed index
                failedNextIndex = Integer.MAX_VALUE;

                // Update member raft variables
                int oldCommitIndex = CoreMember.this.commitIndex;
                CoreMember.this.commitIndex = commitIndex;
                leaderId = fromMemberId;

                if (message.isHeartbeat()) {
                    // Don't save heartbeats. Reuse this message for the heartbeat reply
                    message.appendEntryReply(memberId, currentTerm, savedRid.index, true, true);
                    peer.send(message);
                    if (commitIndex > oldCommitIndex) {
                        indexUpdated(false, false);
                    }
                } else {
                    // Update with the last rid that will be saved
                    sentRid.set(entryTerm, prevLogIndex + 1);

                    if (lastCommand) {
                        // Send this message to storage after all the batches have been processed.
                        // The save queue will trigger replies as the entries are saved.
                        saveQueue.add(message);
                    }
                }
            }
            return true;
        }

        @Override
        public void requestVoteRequest(Message message, int fromMemberId, int term,
                                       boolean isPrevote, Rid lastRid) throws Exception {
            if (message.tracingInfo != null) {
                logger.info("[{}-{}] recv({}): {}", gondola.getHostId(), memberId, fromMemberId, message.tracingInfo);
            }

            Peer peer = peerMap.get(fromMemberId);
            if (peer == null) {
                logger.error("[{}-{}] Received RV from unknown member {}",
                        gondola.getHostId(), memberId, fromMemberId);
                return;
            }

            // Determine if sender's log is up-to-date
            saveQueue.getLatest(savedRid, false);
            boolean validLog = lastRid.term > savedRid.term
                    || (lastRid.term == savedRid.term && lastRid.index >= savedRid.index);

            if (isPrevote) {
                if (isCandidate()) {
                    message.requestVoteReply(memberId, currentTerm, true, validLog);
                    peer.send(message);
                }
                return;
            }

            // Become a follower if the sender's term is later
            updateCurrentTerm(term, fromMemberId);

            // If currently the leader, send out a heartbeat to try and avoid a disruption
            if (isLeader()) {
                sendHeartbeat(true);
            } else if (!validLog || term < currentTerm || votedFor != -1) {
                String m = String.format("[%s-%d] Rejecting RV from %d. ",
                        gondola.getHostId(), memberId, fromMemberId);
                if (votedFor != -1) {
                    m += String.format("Already voted for %d in term %d", votedFor, term);
                } else {
                    m += String.format("Candidate log=(%d,%d) is behind log=(%d,%d)",
                            lastRid.term, lastRid.index, savedRid.term, savedRid.index);
                }
                logger.info(m);

                // Send rejection
                message.requestVoteReply(memberId, currentTerm, false, false);
                peer.send(message);

                // Set the time for the next prevote
                prevoteTs = clock.now() + (long) ((Math.random() * prevotePeriod));
            } else {
                // Vote for this candidate
                votedFor = fromMemberId;
                logger.info("[{}-{}] Voting for {} in term {}", gondola.getHostId(), memberId, votedFor, currentTerm);
                save(currentTerm, votedFor);
                message.requestVoteReply(memberId, currentTerm, false, true);
                peer.send(message);
            }
        }

        /**
         * Note: prevotes are handled by the peer and are not handled here.
         */
        @Override
        public void requestVoteReply(Message message, int fromMemberId, int term, boolean isPrevote,
                                     boolean voteGranted)
                throws Exception {
            if (message.tracingInfo != null) {
                logger.info("[{}-{}] recv({}): {}", gondola.getHostId(), memberId, fromMemberId, message.tracingInfo);
            }
            Peer peer = peerMap.get(fromMemberId);
            if (peer == null) {
                logger.error("Received rv from unknown member {}", fromMemberId);
                return;
            }

            // Become a follower if the sender's term is later
            updateCurrentTerm(term, fromMemberId);

            if (term >= currentTerm && isCandidate() && voteGranted) {
                if (isPrevote) {
                    if (prevotesOnly) {
                        peer.prevoteGranted = true;

                        // With a majority, send out a request vote
                        long prevotes = peers.stream().filter(p -> p.prevoteGranted).count() + 1;
                        if (prevotes >= majority) {
                            logger.info(
                                    "[{}-{}] Has majority of pre-votes ({}). "
                                            + "Incrementing currentTerm to {} and sending out request vote",
                                    gondola.getHostId(), memberId, prevotes, currentTerm + 1);

                            sendRequestVoteRequest(false);
                        }
                    }
                } else if (!prevotesOnly) {
                    // Process real vote
                    peer.votedTerm = term;
                    peer.votedFor = memberId;
                    if (!isLeader() && votedFor == memberId) {
                        long votes = peers.stream().filter(p -> p.votedTerm == currentTerm && p.votedFor == memberId)
                                .count() + 1;

                        // Inform all other candidates of successful election
                        if (votes >= majority) {
                            becomeLeader();
                        }
                    }
                } else {
                    logger.info("[{}-{}] Reject vote from {} for term {} because currently in prevote phase",
                            gondola.getHostId(), memberId, fromMemberId, term);
                }
            }
        }

    }

    public int getMemberId() {
        return memberId;
    }

    /**
     * Used by tests to get the Member object from a Cluster, which is not public for clients.
     *
     * @param shard non-null Cluster object.
     * @return non-null CoreCmd object.
     */
    public static CoreMember getCoreMember(Shard shard) {
        try {
            Field field = Shard.class.getDeclaredField("cmember");
            field.setAccessible(true);
            return (CoreMember) field.get(shard);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Used to measure latency between two different points in the code.
     */
    public static class Latency {

        AtomicInteger headIndex = new AtomicInteger(Integer.MAX_VALUE);
        int oldHeadIndex;
        long headTs;
        long latencyTime;
        int latencyCount;

        public void head(int index) {
            if (index > oldHeadIndex && headIndex.compareAndSet(Integer.MAX_VALUE, index)) {
                headTs = System.nanoTime();
            }
        }

        public void tail(int index) {
            int i = headIndex.get();
            if (index >= i && headIndex.compareAndSet(i, Integer.MAX_VALUE - 1)) {
                latencyTime += (System.nanoTime() - headTs);
                latencyCount++;
                oldHeadIndex = i;
                headIndex.set(Integer.MAX_VALUE);
            }
        }

        /**
         * Returns the latency in milliseconds.
         */
        public double get() {
            double result = .000001 * latencyTime / latencyCount;
            latencyTime = 0;
            latencyCount = 0;
            return result;
        }
    }

    /* ************************************ slave mode ************************************* */

    int masterId = -1;

    // Is set to null after a call to getSlaveStatus()
    Throwable slaveModeException;

    /**
     * Sets this member to slave mode, to sync up its Raft log to match the specified address. In slave mode: <li> the
     * member contacts the specified address, which is expected to be a leader <li> once successfully connected, the
     * member becomes a follower <li> the member ignores all RequestVote messages <li> the member continues to connect
     * to the specified address
     * <p>
     * If masterAddress is -1, the member leaves slave mode.
     *
     * @param masterId the identity of a leader to sync with.
     */
    public void setSlave(int masterId) throws Exception {
        if (masterId != this.masterId) {
            // Remove the current peers
            for (Peer peer : peers) {
                peer.stop();
            }
            peers.clear();
            peerMap.clear();

            // Prepare to talk to new master
            if (masterId >= 0) {
                // todo: Delete the entire log

                currentTerm = 1;
                save(1, -1);
                becomeFollower(masterId);

                // Create a new peer to the master
                Peer masterPeer = new Peer(gondola, this, masterId);
                peers.add(masterPeer);
                peerMap.put(masterPeer.peerId, masterPeer);
                masterPeer.start();
            } else {
                // Leave slave mode
                // Restore peers
                for (int id : peerIds) {
                    Peer peer = new Peer(gondola, this, id);
                    peer.start();
                    peers.add(peer);
                    peerMap.put(peer.peerId, peer);
                }
                reset();
            }
            this.masterId = masterId;
        }
    }

    /**
     * Returns the current status of the slave.
     *
     * @return null if the member is not in slave mode.
     */
    public Member.SlaveStatus getSlaveStatus() throws Exception {
        Member.SlaveStatus ss = new Member.SlaveStatus();
        ss.memberId = memberId;
        ss.masterId = masterId;
        ss.running = masterId >= 0 && role == Role.FOLLOWER;
        ss.commitIndex = getCommitIndex();
        ss.savedIndex = getSavedIndex();
        ss.exception = slaveModeException;
        if (slaveModeException != null) {
            // Without synchronization, might lose a subsequent exception. It's ok.
            slaveModeException = null;
        }
        if (ss.masterId < 0) {
            return null;
        }
        return ss;
    }

    /**
     * Called by Network when there's a connection possibly from a slave.
     *
     * @param channel
     * @return true if the channel is accepted.
     */
    private boolean acceptSlaveConnection(Channel channel) {
        // If the remote member is part of this shard, reject the request
        List<Config.ConfigMember> shardMembers = gondola.getConfig().getMembersInShard(shard.getShardId());
        for (Config.ConfigMember m : shardMembers) {
            if (m.getMemberId() == channel.getRemoteMemberId()) {
                logger.info("[{}-{}] Slave request from {} rejected since in the same shard",
                            gondola.getHostId(), memberId, channel.getRemoteMemberId());
                return false;
            }
        }

        // If this member is not a leader, reject the request
        if (role != Role.LEADER) {
            logger.info("[{}-{}] Slave request from {} rejected: not a leader ({})",
                        gondola.getHostId(), memberId, channel.getRemoteMemberId(), role);
            return false;
        }

        logger.info("[{}-{}] Slave request from {} accepted",
                    gondola.getHostId(), memberId, channel.getRemoteMemberId());

        Peer slave = new Peer(gondola, this, channel);
        slaves.add(slave);
        slave.start();
        return true;
    }

    /**
     * Called by the leader to remove all the slaves.
     */
    private void shutdownSlaves() {
        for (Peer slave : slaves) {
            slave.stop();
        }
        slaves.clear();
    }
}
