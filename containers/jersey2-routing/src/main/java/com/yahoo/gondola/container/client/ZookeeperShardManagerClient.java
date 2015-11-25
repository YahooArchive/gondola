/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.Utils;
import com.yahoo.gondola.container.client.ZookeeperStat.Status;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.RetryOneTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.yahoo.gondola.container.client.ZookeeperAction.Action.MIGRATE_1;
import static com.yahoo.gondola.container.client.ZookeeperAction.Action.MIGRATE_2;
import static com.yahoo.gondola.container.client.ZookeeperAction.Action.MIGRATE_3;
import static com.yahoo.gondola.container.client.ZookeeperAction.Action.START_SLAVE;
import static com.yahoo.gondola.container.client.ZookeeperAction.Action.STOP_SLAVE;
import static com.yahoo.gondola.container.client.ZookeeperStat.Status.APPROACHED;
import static com.yahoo.gondola.container.client.ZookeeperStat.Status.RUNNING;
import static com.yahoo.gondola.container.client.ZookeeperStat.Status.SYNCED;
import static com.yahoo.gondola.container.client.ZookeeperUtils.ensurePath;

/**
 * Zookeeper shard manager client.
 */
public class ZookeeperShardManagerClient implements ShardManagerClient {

    private String serviceName;
    CuratorFramework client;
    Config config;

    ObjectMapper objectMapper = new ObjectMapper();
    Logger logger = LoggerFactory.getLogger(ZookeeperShardManagerClient.class);
    PathChildrenCache stats;
    boolean tracing = false;

    public ZookeeperShardManagerClient(String serviceName, String connectString, Config config) {
        client = CuratorFrameworkFactory.newClient(connectString, new RetryOneTime(1000));
        client.start();
        this.serviceName = serviceName;
        this.config = config;
        ensurePath(serviceName, client.getZookeeperClient());
        watchZookeeperStats();
        config.registerForUpdates(config1 -> tracing = config.getBoolean("tracing.router"));
    }

    private void watchZookeeperStats() {
        try {
            stats = new PathChildrenCache(client, ZookeeperUtils.statBasePath(serviceName), true);
            stats.start();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot start stats watch service.", e);
        }
    }

    @Override
    public void startObserving(String shardId, String observedShardId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        for (Config.ConfigMember m : config.getMembersInShard(shardId)) {
            setAction(m.getMemberId(), START_SLAVE, shardId, observedShardId, timeoutMs);
        }
    }

    private void setAction(int memberId, ZookeeperAction.Action action, Object... args) {
        try {
            trace("Write action {} to memberId={}", action, memberId);
            ZookeeperAction zookeeperAction = new ZookeeperAction();
            zookeeperAction.memberId = memberId;
            zookeeperAction.action = action;
            zookeeperAction.args = Arrays.asList(args);
            client.setData().forPath(ZookeeperUtils.actionPath(serviceName, memberId),
                                     objectMapper.writeValueAsBytes(zookeeperAction));
        } catch (Exception e) {
            logger.warn("Cannot write action {} to memberId={}", action, memberId);
        }
    }


    @Override
    public void stopObserving(String shardId, String observedShardId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        for (Config.ConfigMember m : config.getMembersInShard(shardId)) {
            setAction(m.getMemberId(), STOP_SLAVE, shardId, observedShardId, timeoutMs);
        }
    }

    @Override
    public void migrateBuckets(Range<Integer> splitRange, String fromShardId, String toShardId, long timeoutMs)
        throws ShardManagerException {
        for (Config.ConfigMember m : config.getMembersInShard(fromShardId)) {
            setAction(m.getMemberId(), MIGRATE_1, splitRange.lowerEndpoint(),
                      splitRange.upperEndpoint(), fromShardId, toShardId, timeoutMs);
        }
    }

    @Override
    public boolean waitSlavesSynced(String shardId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        try {
            return waitSlaveCondition(shardId, timeoutMs, Collections.singletonList(SYNCED));
        } catch (ExecutionException e) {
            logger.warn("Error happened in wait slaves -- msg={}", e.getMessage());
            return false;
        }
    }


    @Override
    public boolean waitSlavesApproaching(String shardId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        try {
            return waitSlaveCondition(shardId, timeoutMs, Arrays.asList(APPROACHED, SYNCED));
        } catch (ExecutionException e) {
            logger.warn("Error happened in wait slaves -- msg={}", e.getMessage());
            return false;
        }
    }

    @Override
    public void setBuckets(Range<Integer> splitRange, String fromShardId, String toShardId, boolean migrationComplete) {
        for (Config.ConfigMember m : config.getMembers()) {
            setAction(m.getMemberId(), !migrationComplete ? MIGRATE_2 : MIGRATE_3, splitRange.lowerEndpoint(),
                      splitRange.upperEndpoint(), fromShardId, toShardId, migrationComplete);
        }
    }

    @Override
    public boolean waitBucketsCondition(Range<Integer> range, String fromShardId, String toShardId, long timeoutMs)
        throws InterruptedException {
        try {
            return Utils.pollingWithTimeout(() -> {
                for (ChildData childData : stats.getCurrentData()) {
                    ZookeeperStat stat = objectMapper.readValue(childData.getData(), ZookeeperStat.class);
                    if (stat.mode != ZookeeperStat.Mode.MIGRATING_2 || !stat.status.equals(RUNNING)) {
                        return false;
                    }
                }
                return true;
            }, timeoutMs / 3, timeoutMs);
        } catch (ExecutionException e) {
            logger.warn("Error while waitBucketCondition, message={}", e.getMessage());
            return false;
        }
    }

    private boolean waitSlaveCondition(String shardId, long timeoutMs, List<Status> statuses)
        throws InterruptedException, ExecutionException {
        return Utils.pollingWithTimeout(() -> {
            for (ChildData childData : stats.getCurrentData()) {
                ZookeeperStat stat = objectMapper.readValue(childData.getData(), ZookeeperStat.class);
                if (!config.getMember(stat.memberId).getShardId().equals(shardId)) {
                    continue;
                }
                if (stat.mode != ZookeeperStat.Mode.SLAVE || !statuses.contains(stat.status)) {
                    return false;
                }
            }
            return true;
        }, timeoutMs / 3, timeoutMs);
    }

    private void trace(String format, Object... args) {
        if (tracing) {
            logger.info(format, args);
        }
    }
}
