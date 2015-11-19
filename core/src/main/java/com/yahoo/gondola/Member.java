/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola;

import com.yahoo.gondola.core.CoreMember;
import com.yahoo.gondola.core.Peer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.function.Consumer;

public class Member {
    final static Logger logger = LoggerFactory.getLogger(Member.class);

    final Gondola gondola;
    final CoreMember cmember;
    final Peer peer;

    Member(Gondola gondola, CoreMember cmember, Peer peer) throws Exception {
        this.gondola = gondola;
        this.cmember = cmember;
        this.peer = peer;
    }

    public int getMemberId() {
        if (peer == null) {
            return cmember.getMemberId();
        } else {
            return peer.getPeerId();
        }
    }
    
    public boolean isLocal() {
        return peer == null;
    }

    /**
     * The results are valid only if the local member is the leader.
     */
    public boolean isLogUpToDate() throws Exception {
        if (peer == null) {
            return cmember.sentRid.index == cmember.getSavedIndex();
        } else {
            return cmember.sentRid.index == peer.matchIndex;
        }
    }

    /**
     * Returns the commit index for this cluster.
     */
    public boolean isLeader() {
        return getMemberId() == cmember.getLeaderId();
    }

    /**
     * Returns the current known operational state for this member.
     */
    public boolean isOperational() {
        if (peer == null) {
            return true;
        } else {
            return peer.isOperational();
        }
    }

    public InetSocketAddress getAddress() {
        return gondola.getConfig().getAddressForMember(getMemberId());
    }

    /* ************************************ slave mode ************************************* */

    InetSocketAddress masterAddress;

    Consumer<SlaveStatus> updateCallback;

    /**
     * This class conveys the current status of this member.
     */
    public static class SlaveStatus {
        // The id of the current member
        int memberId;
        
        // True if the member is connected to the leader
        boolean running;

        // Non-null if an error occurred while connecting to or retrieving data from the leader.
        // When non-null, running will be false.
        Throwable exception;
    }

    /**
     * Sets this member to slave mode, to sync up its Raft log to match the specified address.
     * In slave mode:
     * <li> the member contacts the specified address, which is expected to be a leader
     * <li> once successfully connected, the member becomes a follower
     * <li> the member ignores all RequestVote messages
     * <li> the member continues to connect to the specified address
     *
     * If masterAddress is -1, this member leaves slave mode.
     *
     * @param masterId the id of the leader to sync with. Set to -1 to leave slave mode.
     * @param updateCallback the possibly-null function called whenever there's a status change.
     */
    public void setSlave(int masterId, Consumer<SlaveStatus> updateCallback) {
        cmember.setSlave(masterId, updateCallback);
    }

    /**
     * Returns the current status of the slave.
     *
     * @return null if the member is no in slave mode.
     */
    public SlaveStatus getSlaveUpdate() {
        return cmember.getSlaveUpdate();
    }
}
