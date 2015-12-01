/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;

public class ZookeeperServer {
    private TestingServer testingServer;
    private CuratorFramework client = null;
    public ZookeeperServer() {
        try {
            testingServer = new TestingServer();
            testingServer.start();
            client = CuratorFrameworkFactory.newClient(testingServer.getConnectString(), new RetryOneTime(1000));
            client.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void reset() throws Exception {
        try {
            for (String path : client.getChildren().forPath("/")) {
                if (!path.equals("zookeeper")) {
                    client.delete().deletingChildrenIfNeeded().forPath("/" + path);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getConnectString() {
        return testingServer.getConnectString();
    }

    public CuratorFramework getClient() {
        return client;
    }
}
