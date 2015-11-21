/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.impl;

import com.yahoo.gondola.Channel;
import com.yahoo.gondola.Gondola;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketException;

/**
 * Class that wraps a SocketNetwork class and causes random failures to clients.
 */
public class NastyNetwork extends SocketNetwork {
    final static Logger logger = LoggerFactory.getLogger(NastyNetwork.class);

    boolean enabled;

    public NastyNetwork(Gondola gondola, String hostId) throws SocketException {
        super(gondola, hostId);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void enable(boolean on) {
        for (Channel c : channels) {
            ((NastyChannel) c).enable(on);
        }
        enabled = on;
    }
    /**
     * ***************** methods ******************
     */

    @Override
    public Channel createChannel(int fromMemberId, int toMemberId) {
        SocketChannel channel = new NastyChannel(gondola, fromMemberId, toMemberId);
        channels.add(channel);
        return channel;
    }
}
