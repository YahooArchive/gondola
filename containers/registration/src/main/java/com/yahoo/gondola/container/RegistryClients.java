/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.gondola.Config;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;

import java.io.IOException;

/**
 * The Factory class for creating registry client easily.
 */
public class RegistryClients {

    /**
     * prevening create instance of this factory class.
     */
    private RegistryClients() {
    }

    /**
     * create zookeeper client via config.
     * @param config The Gondola config
     * @return The ZookeeperRegistryClient instance
     */
    public static RegistryClient createZookeeperClient(Config config) throws IOException {
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(getZookeeperConnectionString(config))
            .retryPolicy(new RetryOneTime(1000))
            .build();
        client.start();
        return new ZookeeperRegistryClient(client, new ObjectMapper(), config);
    }

    private static String getZookeeperConnectionString(Config config) {
        return String.join(",", config.getList("registry_zookeeper.servers"));
    }
}
