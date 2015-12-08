/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.container.client.ShardManagerClient;
import com.yahoo.gondola.container.client.ZookeeperShardManagerClient;
import com.yahoo.gondola.container.impl.ZookeeperShardManagerServer;

/**
 * A Provider that provide the CommandListener implementation.
 */
public class ShardManagerProvider {

    Gondola gondola;
    Config config;
    RoutingFilter filter;


    public ShardManagerProvider(RoutingFilter filter) {
        this.filter = filter;
        this.gondola = filter.getGondola();
        this.config = gondola.getConfig();
    }

    public ShardManagerServer getShardManagerServer() {
        Utils.RegistryConfig conf = Utils.getRegistryConfig(gondola.getConfig());
        ShardManager shardManager =
            new ShardManager(gondola, filter, gondola.getConfig(),
                             getShardManagerClient());
        switch (conf.type) {
            case NONE:
                return null;
            case ZOOKEEPER:
                ZookeeperShardManagerServer
                    zookeeperShardManagerServer =
                    new ZookeeperShardManagerServer(conf.attributes.get("serviceName"),
                                                    conf.attributes.get("connectString"),
                                                    gondola, shardManager);
                return zookeeperShardManagerServer;
        }
        throw new IllegalArgumentException("Unknown config");
    }

    public ShardManagerClient getShardManagerClient() {
        Utils.RegistryConfig conf = Utils.getRegistryConfig(gondola.getConfig());
        switch (conf.type) {
            case NONE:
                return null;
            case ZOOKEEPER:
                return new ZookeeperShardManagerClient(conf.attributes.get("serviceName"),
                                                       gondola.getHostId(),
                                                       conf.attributes.get("connectString"),
                                                       gondola.getConfig());
        }
        throw new IllegalArgumentException("Unknown config");
    }
}
