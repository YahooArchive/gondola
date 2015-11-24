/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.client;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.utils.EnsurePath;

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

    public static String statPath(String serviceName, int memberId) {
        return statBasePath(serviceName) + "/" + memberId;
    }

    public static String statBasePath(String serviceName) {
        return basePath(serviceName) + "/stats";
    }

    public static void ensurePath(String serviceName, CuratorZookeeperClient client) {
        try {
            new EnsurePath(basePath(serviceName)).ensure(client);
            new EnsurePath(actionBasePath(serviceName)).ensure(client);
            new EnsurePath(statBasePath(serviceName)).ensure(client);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static String basePath(String serviceName) {
        return "/" + serviceName + PREFIX;
    }
}
