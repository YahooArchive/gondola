/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola;

import java.net.InetSocketAddress;

public class NotLeaderException extends Exception {
    InetSocketAddress leaderAddr;

    public NotLeaderException(InetSocketAddress leaderAddr) {
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
