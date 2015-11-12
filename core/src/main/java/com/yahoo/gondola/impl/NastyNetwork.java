/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.impl;

import com.yahoo.gondola.Channel;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.Network;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
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
