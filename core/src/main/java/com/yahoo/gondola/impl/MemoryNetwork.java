/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.impl;

import com.yahoo.gondola.Channel;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.Network;

import java.util.LinkedList;
import java.util.List;

public class MemoryNetwork implements Network {
    List<Channel> channels = new LinkedList<>();
    private Gondola gondola;
    String hostId;

    public MemoryNetwork(Gondola gondola, String hostId) {
        this.gondola = gondola;
        this.hostId = hostId;
    }

    @Override
    public Channel createChannel(int fromMemberId, int toMemberId) {
        MemoryChannel channel = new MemoryChannel(gondola, fromMemberId, toMemberId);
        channels.add(channel);
        return channel;
    }

    @Override
    public String getAddress() {
        return hostId;
    }

    @Override
    public boolean isActive(String address) {
        return getAddress().equals(address);
    }

    @Override
    public List<Channel> getChannels() {
        return channels;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
}
