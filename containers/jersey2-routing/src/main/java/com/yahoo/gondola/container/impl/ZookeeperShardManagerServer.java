/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.container.AdminClient;
import com.yahoo.gondola.container.ShardManager;
import com.yahoo.gondola.container.ShardManagerProtocol;
import com.yahoo.gondola.container.ShardManagerServer;
import com.yahoo.gondola.container.client.ZookeeperAction;
import com.yahoo.gondola.container.client.ZookeeperStat;
import com.yahoo.gondola.container.client.ZookeeperUtils;
import com.yahoo.gondola.core.Utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.yahoo.gondola.container.client.ZookeeperStat.Mode.MIGRATING_1;
import static com.yahoo.gondola.container.client.ZookeeperStat.Mode.MIGRATING_2;
import static com.yahoo.gondola.container.client.ZookeeperStat.Mode.NORMAL;
import static com.yahoo.gondola.container.client.ZookeeperStat.Mode.SLAVE;
import static com.yahoo.gondola.container.client.ZookeeperStat.Status.APPROACHED;
import static com.yahoo.gondola.container.client.ZookeeperStat.Status.FAILED;
import static com.yahoo.gondola.container.client.ZookeeperStat.Status.RUNNING;
import static com.yahoo.gondola.container.client.ZookeeperStat.Status.SYNCED;
import static com.yahoo.gondola.container.client.ZookeeperUtils.actionPath;
import static com.yahoo.gondola.container.client.ZookeeperUtils.ensurePath;
import static com.yahoo.gondola.container.client.ZookeeperUtils.statPath;

/**
 * Shard manager server implementation using ZooKeeper.
 */
public class ZookeeperShardManagerServer implements ShardManagerServer {

    private static final long RETRY_WAIT_TIME = AdminClient.TIMEOUT_MS / AdminClient.RETRY_COUNT;
    private static final int RETRY_TIME = 3;
    private CuratorFramework client;
    private ShardManager delegate;
    private String serviceName;
    private Gondola gondola;
    private Config config;
    ObjectMapper objectMapper = new ObjectMapper();
    Logger logger = LoggerFactory.getLogger(ZookeeperShardManagerServer.class);
    List<NodeCache> nodes = new ArrayList<>();
    Map<Integer, ZookeeperStat> currentStats = new HashMap<>();
    Map<Integer, ZookeeperAction> actions = new ConcurrentHashMap<>();
    List<Thread> threads = new ArrayList<>();
    boolean tracing = false;

    public ZookeeperShardManagerServer(String serviceName, String connectString, Gondola gondola, ShardManager shardmanager) {
        this.serviceName = serviceName;
        this.gondola = gondola;
        this.delegate = shardmanager;
        config = gondola.getConfig();
        config.registerForUpdates(config -> tracing = config.getBoolean("tracing.router"));
        client = CuratorFrameworkFactory.newClient(connectString, new RetryOneTime(1000));
        client.start();
        Watcher watcher = new Watcher();
        watcher.start();
        threads.add(watcher);
        initNode(gondola.getHostId());
    }

    private void initNode(String hostId) {
        ensurePath(serviceName, client);
        for (Config.ConfigMember member : config.getMembersInHost(hostId)) {
            try {
                trace("[{}-{}] Initializing zookeeper node...", gondola.getHostId(), member.getMemberId());
                String actionPath = actionPath(serviceName, member.getMemberId());
                String statPath = statPath(serviceName, member.getMemberId());
                ZookeeperStat stat;
                ZookeeperAction action;
                try {
                    stat = objectMapper.readValue(client.getData().forPath(statPath), ZookeeperStat.class);
                } catch (Exception e) {
                    stat = new ZookeeperStat();
                    stat.memberId = member.getMemberId();
                    stat.shardId = member.getShardId();
                    client.create().creatingParentContainersIfNeeded()
                        .forPath(statPath, objectMapper.writeValueAsBytes(stat));
                }

                try {
                    action = objectMapper.readValue(client.getData().forPath(actionPath), ZookeeperAction.class);
                } catch (Exception e) {
                    action = new ZookeeperAction();
                    action.memberId = member.getMemberId();
                    client.create().creatingParentContainersIfNeeded()
                        .forPath(actionPath, objectMapper.writeValueAsBytes(action));
                }
                currentStats.put(stat.memberId, stat);
                actions.put(action.memberId, action);
                resume(stat, action);
                NodeCache node = new NodeCache(client, actionPath);
                node.getListenable().addListener(getListener(node, stat));
                node.start();
                nodes.add(node);
            } catch (Exception e) {
                logger.warn("[{}-{}] Unable to create member node, msg={}",
                            gondola.getHostId(), member.getMemberId(), e.getMessage(), e);
            }
        }
    }

    class Watcher extends Thread {

        @Override
        public void run() {
            setName("Zookeeper-Watcher");
            while (true) {
                try {
                    for (Map.Entry<Integer, ZookeeperStat> e : currentStats.entrySet()) {
                        ZookeeperStat stat = e.getValue();
                        ZookeeperAction action = actions.get(e.getKey());
                        watchAction(action, stat);
                    }
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    logger.error("[{}] Unexpected error - {}", gondola.getHostId(), e.getMessage());
                }
            }
        }

        private void watchAction(ZookeeperAction action, ZookeeperStat stat)
            throws InterruptedException {
            ZookeeperAction.Args args = action.parseArgs();
            ZookeeperStat.Status origStatus = stat.status;
            switch (stat.mode) {
                case NORMAL:
                case MIGRATING_1:
                case MIGRATING_2:
                    break;
                case SLAVE:
                    if (action.action.equals(ZookeeperAction.Action.START_SLAVE)) {
                        try {
                            if (delegate.waitSlavesSynced(args.fromShard, 0)) {
                                stat.status = SYNCED;
                            } else if (delegate.waitSlavesApproaching(args.fromShard, 0)) {
                                stat.status = APPROACHED;
                            } else {
                                stat.status = RUNNING;
                            }
                        } catch (ShardManagerProtocol.ShardManagerException e) {
                            stat.status = FAILED;
                            stat.reason = e.getMessage();
                        }
                    }
                    break;
            }
            if (origStatus != stat.status) {
                writeStat(stat.memberId, stat);
            }
        }
    }

    private void writeStat(Integer memberId, ZookeeperStat zookeeperStat) {
        trace("[{}-{}] Update stat stat={}",
              gondola.getHostId(), memberId, zookeeperStat);
        try {
            client.setData().forPath(ZookeeperUtils.statPath(serviceName, memberId),
                                                    objectMapper.writeValueAsBytes(zookeeperStat));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.warn("[{}-{}] Write stat failed, reason={}", gondola.getHostId(), memberId, e.getMessage());
        }
    }

    private void resume(ZookeeperStat stat, ZookeeperAction action) throws InterruptedException {
        processAction(stat, action);
    }

    private NodeCacheListener getListener(NodeCache node, ZookeeperStat stat) {
        return () -> {
            try {
                if (node.getCurrentData() != null) {
                    ZookeeperAction action = objectMapper.readValue(node.getCurrentData().getData(), ZookeeperAction.class);
                    actions.put(action.memberId, action);
                    processAction(stat, action);
                }
            } catch (Exception e) {
                logger.warn("[{}] Process action error - {}", gondola.getHostId(), e.getMessage());
            }
        };
    }

    private void processAction(ZookeeperStat stat, ZookeeperAction action) throws InterruptedException {
        if (action.action == ZookeeperAction.Action.NOOP) {
            return;
        }
        trace("[{}-{}] Processing action={} args={}", gondola.getHostId(), stat.memberId, action.action, action.args);
        ZookeeperAction.Args args = action.parseArgs();
        for (int i = 0; i < RETRY_TIME; i++) {
            try {
                switch (action.action) {
                    case NOOP:
                        break;
                    case START_SLAVE:
                        delegate.startObserving(args.fromShard, args.toShard, args.timeoutMs);
                        stat.mode = SLAVE;
                        break;
                    case STOP_SLAVE:
                        delegate.stopObserving(args.fromShard, args.toShard, args.timeoutMs);
                        stat.mode = NORMAL;
                        break;
                    case MIGRATE_1:
                        delegate.migrateBuckets(Range.closed(args.rangeStart, args.rangeStop),
                                                args.fromShard, args.toShard, args.timeoutMs);
                        stat.mode = MIGRATING_1;
                        break;
                    case MIGRATE_2:
                        delegate.setBuckets(Range.closed(args.rangeStart, args.rangeStop),
                                            args.fromShard, args.toShard, args.complete);
                        stat.mode = MIGRATING_2;
                        break;
                    case MIGRATE_3:
                        delegate.setBuckets(Range.closed(args.rangeStart, args.rangeStop),
                                            args.fromShard, args.toShard, args.complete);
                        stat.mode = NORMAL;
                        break;
                }
                stat.status = RUNNING;
                stat.reason = null;
            } catch (ShardManagerProtocol.ShardManagerException e) {
                logger.warn("[{}-{}] Cannot execute action={} args={} reason={}",
                            gondola.getHostId(), action.memberId, action, action.args, e.getMessage());
                stat.status = FAILED;
                stat.reason = e.getMessage();
            }
            writeStat(stat.memberId, stat);
            if (stat.status != FAILED) {
                break;
            }
            Thread.sleep(RETRY_WAIT_TIME);
        }
    }

    @Override
    public void stop() {
        nodes.forEach(CloseableUtils::closeQuietly);
        for (NodeCache node : nodes) {
            try {
                node.close();
            } catch (IOException e) {
                // ignored.
                logger.warn("[{}] Close ZK node cache failed. message={}", gondola.getHostId(), e.getMessage());
            }
        }
        Utils.stopThreads(threads);
        CloseableUtils.closeQuietly(client);
    }

    @Override
    public ShardManager getShardManager() {
        return delegate;
    }

    @Override
    public Map getStatus() {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put("currentStats", currentStats);
        map.put("actions", actions);
        return map;
    }

    private void trace(String format, Object... args) {
        if (tracing) {
            logger.info(format, args);
        }
    }
}
