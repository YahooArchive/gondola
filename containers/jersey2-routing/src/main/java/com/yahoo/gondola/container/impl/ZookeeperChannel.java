/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.impl;

import com.yahoo.gondola.container.Channel;
import com.yahoo.gondola.container.client.StatClient;

/**
 * Channel implementation using ZooKeeper.
 */
public class ZookeeperChannel implements Channel, StatClient {

    @Override
    public boolean hasCommand() {
        return false;
    }

    @Override
    public Command recvCommand() {
        return null;
    }

    @Override
    public void sendResult(Result result) {

    }

    @Override
    public boolean waitSynced(String clusterid, long timeoutMs) {
        return false;
    }

    @Override
    public boolean waitApproaching(String clusterId, long timeoutMs) {
        return false;
    }
}
