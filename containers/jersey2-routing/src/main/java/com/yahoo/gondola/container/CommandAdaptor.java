/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.container;

import com.yahoo.gondola.container.client.SnapshotManagerClient;

// TODO: implementation
/**
 * The command adaptor reads from channel related API.
 */
public class CommandAdaptor {
    private Channel channel;
    private SnapshotManagerClient snapshotManagerClient;
    private ShardManager shardManager;

    /**
     * Constructor
     * @param shardManager
     */
    public CommandAdaptor(ShardManager shardManager) {
        this.shardManager = shardManager;
    }
}
