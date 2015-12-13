/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola;

import java.net.InetSocketAddress;

/**
 * The type Not leader exception.
 */
public class NotLeaderException extends GondolaException {
    InetSocketAddress leaderAddr;

    public NotLeaderException(InetSocketAddress leaderAddr) {
        super(Code.NOT_LEADER, String.format(Code.NOT_LEADER.messageTemplate(),
                                             leaderAddr == null ? "unknown" : leaderAddr));
        this.leaderAddr = leaderAddr;
    }

    /**
     * Returns the location of the leader or null if there is no leader at the moment.
     *
     * @return the possibly-null address of the leader.
     */
    public InetSocketAddress getLeaderAddress() {
        return leaderAddr;
    }
}
