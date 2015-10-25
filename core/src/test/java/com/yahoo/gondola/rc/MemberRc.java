/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.rc;

import com.yahoo.gondola.*;
import com.yahoo.gondola.core.CoreMember;
import com.yahoo.gondola.core.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Observable;
import java.util.Observer;

/**
 * This "remote control" class is meant to represent one member in a particular cluster.
 * In reality, it encapsultes the local member of a particular cluster of a particular gondola instance.
 * All operations on this abstract member targets the cluster's local member.
 */
public class MemberRc {
    static Logger logger = LoggerFactory.getLogger(MemberRc.class);

    final GondolaRc rc;
    final Gondola gondola;
    final Cluster cluster;
    final public CoreMember cmember;
    final RcStorage storage;
    final RcNetwork network;

    public MemberRc(GondolaRc rc, Gondola gondola) {
        this.rc = rc;
        this.gondola = gondola;
        cluster = gondola.getCluster("cluster1");
        cmember = CoreMember.getCoreMember(cluster);
        storage = (RcStorage) gondola.getStorage();
        network = (RcNetwork) gondola.getNetwork();
        gondola.registerForRoleChanges(new Observer() {
            @Override
            public void update(Observable obs, Object evt) {
                RoleChangeEvent crevt = (RoleChangeEvent) evt;
            }
        });
    }

    public void reset() throws Exception {
        cmember.reset();
    }

    public Gondola getGondola() {
        return gondola;
    }

    public Cluster getCluster() {
        return cluster;
    }
    
    public int getMemberId() {
        return cmember.getMemberId();
    }

    public Command checkoutCommand() throws Exception {
        return cluster.checkoutCommand();
    }

    public Command getCommittedCommand(int index) throws Exception {
        return cluster.getCommittedCommand(index);
    }

    public Command getCommittedCommand(int index, int timeout) throws Exception {
        return cluster.getCommittedCommand(index, timeout);
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
        network.pauseDeliveryTo(cmember.getMemberId(), pause);
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

    public void registerForRoleChanges(Observer observer) {
        gondola.registerForRoleChanges(observer);
    }

    public void unregisterForRoleChanges(Observer observer) {
        gondola.unregisterForRoleChanges(observer);
    }
}
