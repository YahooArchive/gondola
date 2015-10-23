/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.core;

/**
 * Created by wcpan2 on 5/12/15.
 */
public interface StatsMBean {

    /********************** get *******************/

    int getAppendEntryRequest();

    int getAppendEntryBatch();

    int getAppendEntryReply();

    int getRequestVoteRequest();

    int getRequestVoteReply();

    int getEmptyCommandPool();

    int getSentMessages();

    long getSentBytes();

    int getIncomingMessages();

    long getIncomingBytes();

    long getIncomingQueueFull();

    int getSavedCommands();

    long getSavedBytes();

    /********************** update *******************/

    void hello();

    void appendEntryRequest();

    void appendEntryBatch();

    void appendEntryReply();

    void requestVoteRequest();

    void requestVoteReply();

    void emptyCommandPool();

    void sentMessage(int bytes);

    void incomingMessage(int bytes);

    void incomingQueueFull();

    void savedCommand(int bytes);
}
