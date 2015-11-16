/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola;

import java.net.InetSocketAddress;
import java.util.List;

public class RoleChangeEvent {
    public Shard shard;
    public Member member;

    // Null if the leader is currently unknown
    public Member leader;
    public Role oldRole;
    public Role newRole;

    public RoleChangeEvent(Shard shard, Member member, Member leader, Role oldRole, Role newRole) {
        this.shard = shard;
        this.member = member;
        this.leader = leader;
        this.oldRole = oldRole;
        this.newRole = newRole;
    }
}
