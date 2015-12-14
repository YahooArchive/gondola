/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.client;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.RetryForever;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The type Zookeeper utils.
 */
public class ZookeeperUtils {

    static Map<String, CuratorFramework> clients = new ConcurrentHashMap<>();

    public static CuratorFramework getCuratorFrameworkInstance (String connectString) {
        CuratorFramework client = clients.get(connectString);
        if (client == null || !client.getState().equals(CuratorFrameworkState.STARTED)) {
            CuratorFramework newClient = CuratorFrameworkFactory.newClient(connectString, new RetryForever(1000));
            newClient.start();
            CuratorFramework origClient = clients.putIfAbsent(connectString, newClient);
            client = origClient != null ? origClient : newClient;
            if (origClient != null) {
                newClient.close();
            }
        }
        return client;
    }
    public static String actionPath(String serviceName, int memberId) {
        return actionBasePath(serviceName) + "/" + memberId;
    }

    public static String actionBasePath(String serviceName) {
        return basePath(serviceName) + "/actions";
    }

    public static String configPath(String serviceName) {
        return basePath(serviceName) + "/config";
    }

    public static String statPath(String serviceName, int memberId) {
        return statBasePath(serviceName) + "/" + memberId;
    }

    public static String statBasePath(String serviceName) {
        return basePath(serviceName) + "/stats";
    }

    public static void ensurePath(String serviceName, CuratorFramework client) {
        try {
            client.checkExists().creatingParentContainersIfNeeded().forPath(basePath(serviceName));
            client.checkExists().creatingParentContainersIfNeeded().forPath(actionBasePath(serviceName));
            client.checkExists().creatingParentContainersIfNeeded().forPath(statBasePath(serviceName));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static String basePath(String serviceName) {
        return "/" + serviceName;
    }
}
