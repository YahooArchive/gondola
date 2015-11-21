/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola;

import java.util.List;
import java.util.function.Function;

/**
 * This class provides communication between the local member and remote members.
 * More specifically, this class is used to create a channel between the local and remote members
 * and the channel object provides streams which are used to send and receive data to remote members.
 */
public interface Network extends Stoppable {
    /**
     * Creates a communication channel between the local member and a remote member.
     *
     * @param fromMemberId the id of the local member
     * @param toMemberId the id of the remote member
     * @return a non-null channel between fromMemberId and toMemberId
     */
    public Channel createChannel(int fromMemberId, int toMemberId);

    /**
     * Registers a callback which will be called for new connections that don't match any created channels.
     *
     * @param listener a non-null function that takes a channel and returns true the channel is valid.
     */
    public void register(Function<Channel, Boolean> listener);

    /**
     * Returns a string that can be used to contact the local member. The storage system
     * uses this string to help prevent multiple processes from accessing the database.
     *
     * @return An immutable non-null string representing the local member.
     */
    public String getAddress();

    /**
     * Returns true if the member at the given address is active.
     *
     * @param address non-null string that was returned from getAddress().
     * @return The active state of the member at the specified address.
     */
    public boolean isActive(String address);

    /**
     * Returns a list of created channels.
     *
     * @return a non-null list of created channels.
     */
    public List<Channel> getChannels();
}
