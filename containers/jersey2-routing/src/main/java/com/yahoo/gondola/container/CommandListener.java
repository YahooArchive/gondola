/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

/**
 * Listens to the admin command
 */
public interface CommandListener {

    /**
     * set shard manager handler
     * @param handler
     */
    void setShardManagerHandler (ShardManager handler);
}
