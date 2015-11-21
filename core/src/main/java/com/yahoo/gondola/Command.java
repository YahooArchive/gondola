/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola;

import com.yahoo.gondola.core.CoreCmd;
import com.yahoo.gondola.core.CoreMember;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeoutException;

/**
 * This object is used to commit entries into the Raft log.
 * Usage:
 * Command c = cluster.checkoutCommand();
 * c.commit(buffer, 0, len);
 * c.release();
 * <p>
 * This object is generally not thread-safe; that is, an object of this class should not be called by multiple client
 * threads simultaneously.
 */
public class Command {
    final static Logger logger = LoggerFactory.getLogger(Command.class);

    public final static int STATUS_NONE = -1;
    public final static int STATUS_OK = 0;
    public final static int STATUS_NOT_LEADER = 1;
    public final static int STATUS_INTERRUPTED = 2; // Not used
    public final static int STATUS_ERROR = 3;       // Storage or system error
    public final static int STATUS_TIMEOUT = 4;

    final Shard shard;
    final CoreCmd ccmd;

    Command(Gondola gondola, Shard shard, CoreMember cmember) {
        this.shard = shard;
        ccmd = new CoreCmd(gondola, shard, cmember);
    }

    /**
     * Returns the maximum number of bytes that can be committed.
     */
    public int getCapacity() {
        return ccmd.buffer.length;
    }

    /**
     * Returns the byte buffer holding the command.
     */
    public byte[] getBuffer() {
        return ccmd.buffer;
    }

    /**
     * Returns the size of the command in bytes.
     */
    public int getSize() {
        return ccmd.size;
    }

    /**
     * Returns the index assigned to this command after a commit() or after a Cluster.getCommittedIndex() call.
     */
    public int getIndex() {
        return ccmd.index;
    }

    /**
     * Returns the commitIndex at the time this command is returned to the client. Applies only for commit.
     *
     * @return -1 if the command was not committed.
     */
    public int getCommitIndex() {
        return ccmd.commitIndex;
    }

    public int getStatus() {
        return ccmd.status;
    }

    /**
     * Returns the contents of this command in string form. Assumes UTF-8 encoding.
     */
    public String getString() {
        try {
            return new String(ccmd.buffer, 0, ccmd.size, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            logger.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    /**
     * Equivalent to commit(buf, buffOffset, bufLen, -1).
     *
     * @throws NotLeaderException if the member is not currently the leader.
     */
    public void commit(byte[] buf, int bufOffset, int bufLen)
            throws InterruptedException, NotLeaderException {
        try {
            ccmd.commit(buf, bufOffset, bufLen, -1);
        } catch (TimeoutException e) {
            throw new IllegalStateException(e); // Can't happen
        }
    }

    /**
     * Blocks until the command in buffer has been committed or the timeout has expired.
     * After this call, this command object can be used again.
     *
     * @param timeout If -1, timeout is disabled.
     * @throws NotLeaderException if the member is not currently the leader.
     */
    public void commit(byte[] buf, int bufOffset, int bufLen, int timeout)
            throws InterruptedException, NotLeaderException, TimeoutException {
        ccmd.commit(buf, bufOffset, bufLen, timeout);
    }

    /**
     * Returns this command object back to the pool.
     */
    public void release() {
        shard.checkinCommand(this);
    }
}
