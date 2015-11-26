/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.client.ZookeeperAction.Action;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.RetryOneTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.yahoo.gondola.container.Utils.pollingWithTimeout;
import static com.yahoo.gondola.container.client.ZookeeperAction.Action.MIGRATE_1;
import static com.yahoo.gondola.container.client.ZookeeperAction.Action.MIGRATE_2;
import static com.yahoo.gondola.container.client.ZookeeperAction.Action.MIGRATE_3;
import static com.yahoo.gondola.container.client.ZookeeperAction.Action.START_SLAVE;
import static com.yahoo.gondola.container.client.ZookeeperAction.Action.STOP_SLAVE;
import static com.yahoo.gondola.container.client.ZookeeperStat.Status.APPROACHED;
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
        // Set slave state
        sendActionToShard(shardId, START_SLAVE, shardId, observedShardId, timeoutMs);
        waitCondition(shardId, ZookeeperStat::isSlaveOperational, timeoutMs);
    }

    private boolean waitCondition(String shardId, Function<ZookeeperStat, Boolean> statChecker, long timeoutMs)
        throws InterruptedException, ShardManagerException {
        try {
            if (!pollingWithTimeout(() -> getStatCheckPredicate(shardId, statChecker),
                                    timeoutMs / 3, timeoutMs)) {
                return false;
            }
        } catch (ExecutionException e) {
            throw new ShardManagerException(e);
        }
        return true;
    }


    private boolean getStatCheckPredicate(String shardId, Function<ZookeeperStat, Boolean> statChecker)
        throws java.io.IOException {
        for (ChildData d : stats.getCurrentData()) {
            ZookeeperStat stat = objectMapper.readValue(d.getData(), ZookeeperStat.class);
            if (shardId != null && !isMemberInShard(stat.memberId, shardId)) {
                continue;
            }
            if (!statChecker.apply(stat)) {
                return false;
            }
        }
        return true;
    }

    private boolean isMemberInShard(int memberId, String shardId) {
        return config.getMember(memberId).getShardId().equals(shardId);
    }

    private void sendActionToShard(String shardId, Action action, Object... args) {
        config.getMembersInShard(shardId).forEach(m -> sendAction(m.getMemberId(), action, args));
    }

    private void sendAction(int memberId, Action action, Object... args) {
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
        sendActionToShard(shardId, STOP_SLAVE, shardId, observedShardId, timeoutMs);
        waitCondition(shardId, ZookeeperStat::isNormalOperational, timeoutMs);
    }


    @Override
    public void migrateBuckets(Range<Integer> splitRange, String fromShardId, String toShardId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        sendActionToShard(fromShardId, MIGRATE_1, splitRange.lowerEndpoint(),
                          splitRange.upperEndpoint(), fromShardId, toShardId, timeoutMs);
        waitCondition(fromShardId, ZookeeperStat::isMigrating1Operational, timeoutMs);
        setBuckets(splitRange, fromShardId, toShardId, true);
    }

    @Override
    public boolean waitSlavesSynced(String shardId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        return waitCondition(shardId, stat -> stat.isSlaveOperational() && stat.status == SYNCED, timeoutMs);
    }


    @Override
    public boolean waitSlavesApproaching(String shardId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        return waitCondition(shardId,
                             stat -> stat.isSlaveOperational() && Arrays.asList(APPROACHED, SYNCED)
                                 .contains(stat.status),
                             timeoutMs);
    }

    @Override
    public void setBuckets(Range<Integer> splitRange, String fromShardId, String toShardId, boolean migrationComplete)
        throws ShardManagerException, InterruptedException {
        sendActionToAll(!migrationComplete ? MIGRATE_2 : MIGRATE_3, splitRange.lowerEndpoint(),
                        splitRange.upperEndpoint(), fromShardId, toShardId, migrationComplete);
        if (!migrationComplete) {
            waitCondition(null, ZookeeperStat::isMigrating2Operational, 1000);
        } else {
            waitCondition(null, ZookeeperStat::isNormalOperational, 1000);
        }
    }

    private void sendActionToAll(Action action, Object... args) {
        for (Config.ConfigMember m : config.getMembers()) {
            sendAction(m.getMemberId(), action, args);
        }
    }

    private void trace(String format, Object... args) {
        if (tracing) {
            logger.info(format, args);
        }
    }
}
