package com.yahoo.gondola.container;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.gondola.Config;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;

public class RegistryClients {
    public static RegistryClient createZookeeperClient(Config config) {
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(getZookeeperConnectionString(config))
            .retryPolicy(new RetryOneTime(1000))
            .build();
        client.start();
        ObjectMapper objectMapper = new ObjectMapper();
        return new ZookeeperRegistryClient(client, objectMapper, config);
    }

    private static String getZookeeperConnectionString(Config config) {
        // TODO: implement
        return "";
    }
}
