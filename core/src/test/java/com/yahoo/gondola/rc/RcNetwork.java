/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.rc;

import com.yahoo.gondola.Channel;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.core.Message;
import com.yahoo.gondola.Network;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class RcNetwork implements Network {
    final static Logger logger = LoggerFactory.getLogger(RcNetwork.class);

    final Gondola gondola;
    final String hostId;

    static Queue<RcChannel> channels = new ConcurrentLinkedQueue<>();

    public RcNetwork(Gondola gondola, String hostId) {
        this.gondola = gondola;
        this.hostId = hostId;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() {
        channels.clear();
    }

    @Override
    public Channel createChannel(int fromMemberId, int toMemberId) {
        RcChannel channel = new RcChannel(gondola, fromMemberId, toMemberId);
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
        return channels.stream().collect(Collectors.toList());
    }

    RcChannel getChannel(int fromMemberId, int toMemberId) {
        for (RcChannel c : channels) {
            if (c.memberId == fromMemberId && c.peerId == toMemberId) {
                return c;
            }
        }
        return null;
    }

    public void pauseDeliveryTo(int memberId, boolean pause) throws Exception {
        for (RcChannel c : channels) {
            if (c.peerId == memberId) {
                c.pauseDelivery(pause);
            }
        }
    }
}
