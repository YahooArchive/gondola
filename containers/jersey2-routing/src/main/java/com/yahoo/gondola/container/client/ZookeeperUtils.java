/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.client;

import org.apache.curator.framework.CuratorFramework;

/**
 * The type Zookeeper utils.
 */
public class ZookeeperUtils {

    private static String PREFIX = "/commands";

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
        return "/" + serviceName + PREFIX;
    }
}
