/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

/**
 * Channel is the interface send/recv command to remote system.
 */
public interface Channel {

    /**
     * Has more command in the channel.
     * @return
     */
    boolean hasCommand();

    /**
     * Receive command from channel.
     *
     * @return
     */
    Command recvCommand();

    /**
     * Send result to channel.
     *
     * @param result
     */
    void sendResult(Result result);

    /**
     * The command/result types.
     */
    enum Type {
        START_SNAPSHOT, STOP_SNAPSHOT, SNAPSHOT_STATUS, RESTORE_SNAPSHOT,
    }

    /**
     * Command.
     */
    class Command {
        Type type;
    }

    /**
     * Result.
     */
    class Result {
        Type type;
    }
}
