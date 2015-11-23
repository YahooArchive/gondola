/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.rc;

import com.yahoo.gondola.Channel;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.core.Message;
import com.yahoo.gondola.Network;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RcNetwork implements Network {
    final static Logger logger = LoggerFactory.getLogger(RcNetwork.class);

    final Gondola gondola;
    final String hostId;

    static Queue<RcChannel> channels = new ConcurrentLinkedQueue<>();

    // Map of all registered listeners
    static Map<Integer, Listener> listeners = new ConcurrentHashMap<>();

    public RcNetwork(Gondola gondola, String hostId) {
        this.gondola = gondola;
        this.hostId = hostId;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public boolean stop() {
        channels.clear();
        return true;
    }

    @Override
    public Channel createChannel(int fromMemberId, int toMemberId) {
        RcChannel channel = new RcChannel(gondola, fromMemberId, toMemberId);
        channels.add(channel);

        Config config = gondola.getConfig();
        boolean isSlave = config.getMember(fromMemberId).getShardId() != config.getMember(toMemberId).getShardId();
        if (isSlave && listeners.size() > 0) {
            // Create a channel in the opposite direction
            RcChannel remoteChannel = new RcChannel(gondola, toMemberId, fromMemberId);
            channels.add(remoteChannel);

            // Send to all listeners
            if (!sendToListeners(toMemberId, remoteChannel)) {
                channel.stop();
            }
        }
        return channel;
    }

    static class Listener {
        int memberId;
        Function<Channel, Boolean> listener;

        Listener(int memberId, Function<Channel, Boolean> listener) {
            this.memberId = memberId;
            this.listener = listener;
        }
    }

    @Override
    public void register(int memberId, Function<Channel, Boolean> listener) {
        Listener l = new Listener(memberId, listener);
        listeners.put(memberId, l);
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

    private boolean sendToListeners(int memberId, Channel channel) {
        Listener l = listeners.get(memberId);
        if (l.listener.apply(channel)) {
            return true;
        }
        return false;
    }
}
