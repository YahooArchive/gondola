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
}
