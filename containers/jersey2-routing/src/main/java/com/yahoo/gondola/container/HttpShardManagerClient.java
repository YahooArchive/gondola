/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.client.ShardManagerClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

/**
 * Shard manager client - HTTP.
 */
public class HttpShardManagerClient implements ShardManagerClient {

    Config config;
    Client client = ClientBuilder.newClient();
    Logger logger = LoggerFactory.getLogger(GondolaAdminClient.class);
    static String PREFIX = "/api/gondola/shardManager/v1/";

    public HttpShardManagerClient(Config config) {
        this.config = config;
    }

    @Override
    public void stop() {

    }

    @Override
    public void startObserving(String shardId, String observedShardId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        for (Config.ConfigMember m : config.getMembersInShard(shardId)) {
            String appUri = Utils.getAppUri(config, m.getHostId());
            Response response = client.target(appUri + PREFIX + "startObserving")
                .queryParam("shardId", shardId)
                .queryParam("observedShardId", observedShardId)
                .queryParam("timeoutMs", timeoutMs)
                .request().post(null);
        }
    }

    @Override
    public void stopObserving(String shardId, String observedShardId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        for (Config.ConfigMember m : config.getMembersInShard(shardId)) {
            String appUri = Utils.getAppUri(config, m.getHostId());
            client.target(appUri + PREFIX + "stopObserving")
                .queryParam("shardId", shardId)
                .queryParam("observedShardId", observedShardId)
                .queryParam("timeoutMs", timeoutMs)
                .request().get();
        }
    }

    @Override
    public void migrateBuckets(Range<Integer> splitRange, String fromShardId, String toShardId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        for (Config.ConfigMember m : config.getMembersInShard(fromShardId)) {
            String appUri = Utils.getAppUri(config, m.getHostId());
            client.target(appUri + PREFIX + "migrateBuckets")
                .queryParam("lowerBound", splitRange.lowerEndpoint())
                .queryParam("upperBound", splitRange.upperEndpoint())
                .queryParam("fromShardId", fromShardId)
                .queryParam("toShardId", toShardId)
                .queryParam("timeoutMs", timeoutMs)
                .request().get();
        }
    }

    @Override
    public boolean waitSlavesSynced(String shardId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        for (Config.ConfigMember m : config.getMembersInShard(shardId)) {
            String appUri = Utils.getAppUri(config, m.getHostId());
            if (!client.target(appUri + PREFIX + "waitSlavesSynced")
                .queryParam("shardId", shardId)
                .queryParam("timeoutMs", timeoutMs)
                .request().get(Boolean.class)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean waitSlavesApproaching(String shardId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        for (Config.ConfigMember m : config.getMembersInShard(shardId)) {
            String appUri = Utils.getAppUri(config, m.getHostId());
            if (!client.target(appUri + PREFIX + "waitSlavesApproaching")
                .queryParam("shardId", shardId)
                .queryParam("timeoutMs", timeoutMs)
                .request().get(Boolean.class)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void setBuckets(Range<Integer> splitRange, String fromShardId, String toShardId, boolean migrationComplete)
        throws ShardManagerException, InterruptedException {
        for (Config.ConfigMember m : config.getMembers()) {
            String appUri = Utils.getAppUri(config, m.getHostId());
            client.target(appUri + PREFIX + "setBuckets")
                .queryParam("lowerBound", splitRange.lowerEndpoint())
                .queryParam("upperBound", splitRange.upperEndpoint())
                .queryParam("fromShardId", fromShardId)
                .queryParam("toShardId", toShardId)
                .queryParam("migrationComplete", migrationComplete)
                .request().get();
        }
    }
}
