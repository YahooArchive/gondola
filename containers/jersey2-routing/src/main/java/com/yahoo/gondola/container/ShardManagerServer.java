/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

/**
 * Listens to the admin command.
 */
public interface ShardManagerServer {

    /**
     * set shard manager handler.
     * @param shardManager
     */
    void setShardManager(ShardManager shardManager);

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
}
