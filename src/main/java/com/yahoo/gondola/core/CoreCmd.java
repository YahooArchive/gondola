package com.yahoo.gondola.core;

import com.yahoo.gondola.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 */
public class CoreCmd {
    final static Logger logger = LoggerFactory.getLogger(CoreCmd.class);

    final Gondola gondola;
    final Cluster cluster;
    final CoreMember cmember;
    final Stats stats;
    final static AtomicInteger commitCounter = new AtomicInteger();

    int counter;
    final static CoreMember.Latency commitLatency = new CoreMember.Latency();

    // Only status needs to be properly protected by this lock.
    final ReentrantLock lock = new ReentrantLock();

    // Clients waiting to commit or retrieve a command block on this condition variable.
    // Blocking continues until the status is not NONE.
    final Condition updateCond = lock.newCondition();

    // These are assigned when the command is waiting to be committed and also when command is retrieved from the log
    public int term;
    public int index;

    // Results to return to the waiter. Initialized in reset().
    public volatile int status;
    public int leaderId;
    public int commitIndex;

    // Non-null when status is ERROR
    String errorMessage;

    // Holds the command
    public byte[] buffer;

    // Number of these objects created
    public static AtomicInteger createdCount = new AtomicInteger();

    // Number of bytes in buffer.
    public int size;

    // Config variables
    static boolean commandTracing;
    static int maxCommandSize;

    public CoreCmd(Gondola gondola, Cluster cluster, CoreMember cmember) {
        this.gondola = gondola;
        this.cluster = cluster;
        this.cmember = cmember;

        createdCount.incrementAndGet();
        stats = gondola.getStats();
        buffer = new byte[maxCommandSize];
        reset();
    }

    /**
     * Must be called before command objects are created.
     */
    public static void initConfig(Config config) {
        config.registerForUpdates(new Observer() {
            /*
             * Called at the time of registration and whenever the config file changes.
             */
            @Override
            public void update(Observable obs, Object arg) {
                Config config = (Config) arg;
                commandTracing = config.getBoolean("tracing.command");
                maxCommandSize = config.getInt("raft.command_max_size");
            }
        });
    }

    /**
     * Resets this command to it's initial state, ready to be used again.
     */
    void reset() {
        status = Command.STATUS_NONE;
        size = 0;
        leaderId = -1;
        commitIndex = -1;
        term = -1;
        index = -1;
        counter = 0;
    }

    /**
     * Blocks until the command in buffer has been committed or the timeout has expired.
     * After this call, this command object can be used again.
     *
     * @param timeout if -1, the timeout is disabled.
     * @throws NotLeaderException if the member is not currently the leader.
     * @throws TimeoutException   if timeout >= 0 and a timeout occurred.
     */
    public void commit(byte[] buf, int bufOffset, int bufLen, int timeout) 
            throws InterruptedException, NotLeaderException, TimeoutException {
        if (bufLen + bufOffset > buffer.length) {
            throw new IllegalStateException(
                    String.format("Command buffer is not large enough. bytes=%d + offset=%d > capacity=%d",
                            bufLen, bufOffset, buffer.length));
        }
        reset();
        counter = commitCounter.incrementAndGet();
        commitLatency.head(counter);
        System.arraycopy(buf, bufOffset, buffer, 0, bufLen);
        size = bufLen;

        // Add to the queue to be processed. Member will call update() when the commit index is advanced.
        cmember.addCommand(this);

        // Wait for updates
        long endTs = timeout < 0 ? Long.MAX_VALUE : gondola.getClock().now() + timeout;
        lock.lock();
        try {
            while (status == Command.STATUS_NONE && gondola.getClock().now() < endTs) {
                if (timeout < 0) {
                    updateCond.await();
                } else {
                    updateCond.await(timeout, TimeUnit.MILLISECONDS);
                }
            }

            // A simple check in case the command was released prematurely
            if (size != bufLen) {
                logger.error("This command object is being used by another thread");
            }
            //commitLatency.tail(counter);
        } finally {
            lock.unlock();
        }

        switch (status) {
            case Command.STATUS_NONE:
                // Timeout occurred
                status = Command.STATUS_TIMEOUT;
                throw new TimeoutException(String.format("Timeout (%d ms) for index %d size %d",
                                                         timeout, index, bufLen));
            case Command.STATUS_NOT_LEADER:
                throw new NotLeaderException(leaderId == -1 ? null : gondola.getConfig().getAddressForMember(leaderId));
            case Command.STATUS_ERROR:
                throw new IllegalStateException("Error committing index " + index + ": " + errorMessage);
            case Command.STATUS_OK:
                if (commandTracing) {
                    logger.info("[{}-{}] committed(term={} index={} size={}) status={}",
                                gondola.getHostId(), cmember.memberId, term, index, size, status);
                }
                break;
        }
        commitLatency.tail(counter);
    }

    /**
     * This should not be called by clients.
     * This method is called indirectly via a client call to Cluster.getCommittedCommand(), when the 
     * requested index has not been committed yet.
     * After this call, this command object can be used again.
     *
     * @param index   the index of the log entry.
     * @param timeout return after timeout milliseconds, even if the log entry is not available.
     */
    void waitForLogEntry(int index, int timeout) throws InterruptedException, TimeoutException {
        reset();
        this.index = index;

        // Wait for updates
        long endTs = timeout < 0 ? Long.MAX_VALUE : gondola.getClock().now() + timeout;
        lock.lock();
        try {
            while (status == Command.STATUS_NONE && gondola.getClock().now() < endTs) {
                if (timeout < 0) {
                    updateCond.await();
                } else {
                    updateCond.await(timeout, TimeUnit.MILLISECONDS);
                }
            }

            // A simple check in case the command is being used by another thread
            if (this.index != index) {
                logger.error("This command object is being used by another thread");
            }
        } finally {
            lock.unlock();
        }

        switch (status) {
            case Command.STATUS_NONE:
                // Timeout occurred
                status = Command.STATUS_TIMEOUT;
                throw new TimeoutException(String.format("Timeout (%d ms) for index %d", timeout, index));
            case Command.STATUS_NOT_LEADER:
                assert false; // Can't happen
                break;
            case Command.STATUS_ERROR:
                throw new IllegalStateException("Error getting index " + index + ": " + errorMessage);
            case Command.STATUS_OK:
                break;
        }
    }

    /**
     * Called by CoreMember on command objects that are awaiting state changes. 
     * If errorMessage is not null, all commands whose index is > commitIndex are signaled with an error.
     * Otherwise all commands whose index <= commitIndex are released.
     *
     * @param status  The new status of this command.
     * @param leaderId The member id of the new leader; -1 if not known.
     * @return true if the command should be removed from the wait queue.
     */
    void update(int status, int leaderId) {
        lock.lock();
        try {
            this.status = status;
            this.leaderId = leaderId;
            updateCond.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Used by tests to get the CoreCmd object from a Command, which is not public for clients.
     *
     * @param command non-null Command object.
     * @return non-null CoreCmd object.
     */
    public static CoreCmd getCoreCmd(Command command) {
        try {
            Field field = Command.class.getDeclaredField("ccmd");
            field.setAccessible(true);
            return (CoreCmd) field.get(command);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
