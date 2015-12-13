/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.rc;

import com.yahoo.gondola.Shard;
import com.yahoo.gondola.Command;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.Network;
import com.yahoo.gondola.Role;
import com.yahoo.gondola.RoleChangeEvent;
import com.yahoo.gondola.core.CoreMember;
import com.yahoo.gondola.core.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * This "remote control" class is meant to represent one member in a particular shard.
 * In reality, it encapsultes the local member of a particular shard of a particular gondola instance.
 * All operations on this abstract member targets the shard's local member.
 */
public class MemberRc {
    static Logger logger = LoggerFactory.getLogger(MemberRc.class);

    final GondolaRc rc;
    final Gondola gondola;
    Shard shard;
    public CoreMember cmember;
    RcStorage storage;
    Network network;

    public MemberRc(GondolaRc rc, Gondola gondola) {
        this.rc = rc;
        this.gondola = gondola;
        shard = gondola.getShard("shard1");
        cmember = CoreMember.getCoreMember(shard);
        storage = (RcStorage) gondola.getStorage();
        network = gondola.getNetwork();
    }

    public void reset() throws Exception {
        cmember.reset();
    }

    public Gondola getGondola() {
        return gondola;
    }

    public Shard getShard() {
        return shard;
    }
    
    public int getMemberId() {
        return cmember.getMemberId();
    }

    public int getCommitIndex() {
        return cmember.getCommitIndex();
    }
    
    public void enable(boolean on) throws Exception {
        cmember.enable(on);
    }

    public boolean isEnabled() {
        return cmember.isEnabled();
    }

    public Command checkoutCommand() throws Exception {
        return shard.checkoutCommand();
    }

    public Command getCommittedCommand(int index) throws Exception {
        return shard.getCommittedCommand(index);
    }

    public Command getCommittedCommand(int index, int timeout) throws Exception {
        return shard.getCommittedCommand(index, timeout);
    }

    public void setLeader() throws Exception {
        cmember.becomeLeader();
        cmember.indexUpdated(false, false); // Reset wait time
    }

    public void setCandidate() throws Exception {
        cmember.becomeCandidate();
        cmember.indexUpdated(false, false); // Reset wait time
    }

    public void setFollower() throws Exception {
        cmember.becomeFollower(-1);
        cmember.indexUpdated(false, false); // Reset wait time
    }

    public boolean isLeader() throws Exception {
        return cmember.getRole() == Role.LEADER;
    }

    public void insert(int term, int index, String command) throws Exception {
        byte[] buffer = command.getBytes("UTF-8");
        storage.appendLogEntry(cmember.getMemberId(), term, index, buffer, 0, buffer.length);
    }

    public void saveVote(int currentTerm, int votedFor) throws Exception {
        storage.saveVote(cmember.getMemberId(), currentTerm, votedFor);
    }

    public int getMaxGap() throws Exception {
        return storage.getMaxGap(cmember.getMemberId());
    }

    public void setMaxGap(int maxGap) throws Exception {
        storage.setMaxGap(cmember.getMemberId(), maxGap);
    }

    /**
     * Pauses delivery of messages to this member.
     */
    public void pauseDelivery(boolean pause) throws Exception {
        if (network instanceof RcNetwork) {
            ((RcNetwork) network).pauseDeliveryTo(cmember.getMemberId(), pause);
        }
    }

    public void deliverRequestVoteReply(MemberRc from, int term, boolean isPrevote, boolean voteGranted) throws Exception {
        Message message = gondola.getMessagePool().checkout();
        try {
            message.requestVoteReply(from.getMemberId(), term, isPrevote, voteGranted);
            cmember.addIncoming(message);
        } finally {
            message.release();
        }
    }

    public void showSummary() {
        cmember.showSummary(0, true);
    }

    public void registerForRoleChanges(Consumer<RoleChangeEvent> listener) {
        gondola.registerForRoleChanges(listener);
    }

    public void unregisterForRoleChanges(Consumer<RoleChangeEvent> listener) {
        gondola.unregisterForRoleChanges(listener);
    }
}
