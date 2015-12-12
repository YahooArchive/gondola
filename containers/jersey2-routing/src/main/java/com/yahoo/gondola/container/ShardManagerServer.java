/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import java.util.Map;

/**
 * Listens to the admin command.
 */
public interface ShardManagerServer {

    /**
     * stop shard manager server.
     */
    void stop();

    /**
     * get shard manager instance.
     *
     * @return
     */
    ShardManager getShardManager();

    /**
     * Get current status
     * @return
     */
    Map getStatus();
}
