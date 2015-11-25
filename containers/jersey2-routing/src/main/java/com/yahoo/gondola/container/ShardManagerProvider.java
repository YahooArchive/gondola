/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.client.ShardManagerClient;

/**
 * A Provider that provide the CommandListener implementation.
 */
public class ShardManagerProvider {
    Config config;

    public ShardManagerProvider(Config config) {
        this.config = config;
    }

    public ShardManagerServer getShardManagerServer() {
        // TODO: read from config
        return null;
    }

    public ShardManagerClient getShardManagerClient() {
        // TODO: read from config
        return null;
    }
}
