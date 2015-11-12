/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola;

/**
 * Enum to represent all three Raft roles. Every member must be in one of these three roles.
 */
public enum Role {
    // Represents the leader role
    LEADER,

    // Represents the candidate role
    CANDIDATE,

    // Represents the follower role
    FOLLOWER
}
