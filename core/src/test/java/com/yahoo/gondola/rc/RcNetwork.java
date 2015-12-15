/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.rc;

import com.yahoo.gondola.*;
import com.yahoo.gondola.core.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RcNetwork implements Network {
    final static Logger logger = LoggerFactory.getLogger(RcNetwork.class);

    final Gondola gondola;
    final String hostId;
    // key -> channel
    static Map<String, RcChannel> channels = new ConcurrentHashMap<>();

    // Map of all registered listeners
    static Map<Integer, Listener> listeners = new ConcurrentHashMap<>();

    public RcNetwork(Gondola gondola, String hostId) {
        this.gondola = gondola;
        this.hostId = hostId;
    }

    @Override
    public void start() throws GondolaException {
    }

    @Override
    public boolean stop() {
        channels.clear();
        return true;
    }

    @Override
    public Channel createChannel(int fromMemberId, int toMemberId) {
        String key = key(fromMemberId, toMemberId);
        RcChannel channel = channels.get(key);
        if (channel == null) {
            channel = new RcChannel(gondola, fromMemberId, toMemberId);
            channels.put(key, channel);
        }

        // If the members are not in the same shard, we assume this is a slave request
        Config config = gondola.getConfig();
        boolean isSlave = config.getMember(fromMemberId).getShardId() != config.getMember(toMemberId).getShardId();
        if (isSlave && listeners.size() > 0) {
            // Create a channel in the opposite direction
            RcChannel remoteChannel = new RcChannel(gondola, toMemberId, fromMemberId);

            // Send to all listeners
            Listener l = listeners.get(toMemberId);
            if (l.listener.apply(remoteChannel)) {
                channels.put(key(toMemberId, fromMemberId), remoteChannel);
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
        return new ArrayList<Channel>(channels.values());
    }

    RcChannel getChannel(int fromMemberId, int toMemberId) {
        return (RcChannel) channels.get(key(fromMemberId, toMemberId));
    }

    public void pauseDeliveryTo(int memberId, boolean pause) throws Exception {
        for (RcChannel c : channels.values()) {
            if (c.peerId == memberId) {
                c.pauseDelivery(pause);
            }
        }
    }

    private String key(int fromMemberId, int toMemberId) {
        return String.format("%d-%d", fromMemberId, toMemberId);
    }
}
