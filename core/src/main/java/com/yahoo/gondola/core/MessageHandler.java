/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation methods in this class should not release the message.
 * It is the responsibility of the caller to release the message after these methods return.
 */
public abstract class MessageHandler {
    final static Logger logger = LoggerFactory.getLogger(MessageHandler.class);

    private void notImplemented(Message message, int fromMemberId) throws Exception {
        logger.error("Handler not implemented: type={} size={} from={}", 
                     message.getType(), message.size(), fromMemberId);
        throw new IllegalStateException("not implemented");
    }

    public void pingRequest(Message message, int fromMemberId, long timestamp) throws Exception {
        notImplemented(message, fromMemberId);
    }

    public void pingReply(Message message, int fromMemberId, long timestamp) throws Exception {
        notImplemented(message, fromMemberId);
    }
    
    /**
     * This method is called for every command that has been batched in this message.
     * Do not reuse 'message' to create a reply since it will still be in use until all batches are handled.
     *
     * @param lastCommand is true if there are no more commands in the message.
     * @return true to get the next batch (if any); false to stop getting more batches.
     */
    public boolean appendEntryRequest(Message message, int fromMemberId, int term, 
                                      int prevLogTerm, int prevLogIndex, int commitIndex,
                                      boolean isHeartbeat, int entryTerm, byte[] buffer, int bufferOffset, int bufferLen, 
                                      boolean lastCommand) throws Exception {
        notImplemented(message, fromMemberId);
        return false;
    }

    /**
     * If success is true, matchIndex should be set to mnIndex.
     * If success is false, nextIndex should be set to mnIndex.
     */
    public void appendEntryReply(Message message, int fromMemberId, int term, 
                                 int mnIndex, boolean success) throws Exception {
        notImplemented(message, fromMemberId);
    }

    public void requestVoteRequest(Message message, int fromMemberId, int term, 
                                   boolean isPrevote, Rid lastLogRid) throws Exception {
        notImplemented(message, fromMemberId);
    }

    public void requestVoteReply(Message message, int fromMemberId, int term, 
                                 boolean isPrevote, boolean voteGranted) throws Exception {
        notImplemented(message, fromMemberId);
    }
}
