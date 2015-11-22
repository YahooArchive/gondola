/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.Member;
import com.yahoo.gondola.Shard;
import com.yahoo.gondola.container.client.ShardManagerClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static com.yahoo.gondola.container.ShardManagerProtocol.ShardManagerException.CODE.FAILED_START_SLAVE;
import static com.yahoo.gondola.container.ShardManagerProtocol.ShardManagerException.CODE.FAILED_STOP_SLAVE;
import static com.yahoo.gondola.container.ShardManagerProtocol.ShardManagerException.CODE.NOT_LEADER;

/**
 * The Shard manager.
 */
public class ShardManager implements ShardManagerProtocol {

    // TODO: move to config
    public static final int POLLING_TIMES = 3;
    public static final int LOG_APPROACHING_DIFF = 1000;

    private Config config;
    private RoutingFilter filter;
    private Gondola gondola;

    static Logger logger = LoggerFactory.getLogger(ShardManager.class);

    private Set<String> observedShards = new HashSet<>();

    private ShardManagerClient shardManagerClient;

    boolean tracing = false;

    public ShardManager(Gondola gondola, RoutingFilter filter, Config config, ShardManagerClient shardManagerClient) {
        this.gondola = gondola;
        this.filter = filter;
        this.config = config;
        this.shardManagerClient = shardManagerClient;
        this.config.registerForUpdates(config1 -> tracing = config1.getBoolean("tracing.router"));
    }


    /**
     * Starts observer mode to remote cluster.
     */
    @Override
    public void startObserving(String observedShardId, String shardId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        boolean success = false;
        trace("[{}] Connect to shardId={} as slave", shardId, observedShardId);
        for (Config.ConfigMember m : config.getMembersInShard(shardId)) {
            if (success = setSlave(shardId, m.getMemberId(), timeoutMs)) {
                break;
            }
        }
        if (!success) {
            throw new ShardManagerException(FAILED_START_SLAVE);
        }
        observedShards.add(observedShardId);
    }

    private boolean setSlave(String shardId, int memberId, long timeoutMs) throws InterruptedException {
        gondola.getShard(shardId).getLocalMember().setSlave(memberId);
        long defaultSleep = timeoutMs / POLLING_TIMES;
        long start = System.currentTimeMillis();
        long now = start;
        Member.SlaveStatus status;
        while (true) {
            status = gondola.getShard(shardId).getLocalMember().getSlaveStatus();
            if (!status.running) {
                long remain = timeoutMs - (now - start);
                Thread.sleep(defaultSleep > remain ? defaultSleep : remain);
            } else {
                return status.running;
            }
            now = System.currentTimeMillis();
            if (now - start >= timeoutMs) {
                logger.warn("Failed start observing {} on shard={}, msg={}",
                            memberId, shardId, status.exception != null ? status.exception.getMessage() : "n/a");
                return false;
            }
        }
    }


    /**
     * Stops observer mode to remote cluster, and back to normal mode.
     */
    @Override
    public void stopObserving(String shardId, String observedShardId, long timeoutMs) throws ShardManagerException,
                                                                                             InterruptedException {
        trace("[{}] Disconnect to shardId={} as slave", shardId, observedShardId);
        boolean success = false;
        for (Config.ConfigMember m : config.getMembersInShard(observedShardId)) {
            if (success = unsetSlave(shardId, m.getMemberId(), timeoutMs)) {
                break;
            }
        }
        if (!success) {
            throw new ShardManagerException(FAILED_STOP_SLAVE);
        }
        observedShards.remove(observedShardId);
    }

    private boolean unsetSlave(String shardId, int memberId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        Member.SlaveStatus status = gondola.getShard(shardId).getLocalMember().getSlaveStatus();

        // Not in slave mode, nothing to do.
        if (status == null) {
            return true;
        }

        // Reject if following different leader
        if (status.memberId != memberId) {
            throw new ShardManagerException(FAILED_STOP_SLAVE,
                                            String.format(
                                                "Cannot stop slave due to different master. current=%d, target=%d",
                                                status.memberId, memberId));
        }

        gondola.getShard(shardId).getLocalMember().setSlave(-1);

        long start = System.currentTimeMillis();
        long now = start;
        long defaultSleep = timeoutMs / POLLING_TIMES;

        while (true) {
            if (gondola.getShard(shardId).getLocalMember().getSlaveStatus() != null) {
                long remain = defaultSleep - (now - start);
                Thread.sleep(defaultSleep < remain ? defaultSleep : remain);
                now = System.currentTimeMillis();
                if (now - start >= timeoutMs) {
                    logger.warn("Failed stop observing {} on shard={}", memberId, shardId);
                    return false;
                }
            } else {
                return true;
            }
        }
    }

    /**
     * Splits the bucket of fromCluster, and reassign the buckets to toCluster.
     */
    @Override
    public void migrateBuckets(Range<Integer> splitRange, String fromShardId,
                               String toShardId, long timeoutMs) throws ShardManagerException {
        // Make sure only leader can execute this request.
        if (!filter.isLeaderInShard(fromShardId)) {
            throw new ShardManagerException(NOT_LEADER);
        } else {
            assignBucketOnLeader(splitRange, fromShardId, toShardId, timeoutMs);
        }
    }

    private void assignBucketOnLeader(Range<Integer> splitRange, String fromShardId, String toShardId, long timeoutMs)
        throws ShardManagerException {
        try {
            filter.blockRequestOnBuckets(splitRange);
            filter.waitNoRequestsOnBuckets(splitRange, timeoutMs);
            shardManagerClient.waitSlavesSynced(toShardId, timeoutMs);
            shardManagerClient.stopObserving(toShardId, fromShardId, timeoutMs);
            setBuckets(splitRange, fromShardId, toShardId, false);
        } catch (InterruptedException e) {
            // TODO: rollback
            logger.warn("Error occurred, rollback!", e);
        } finally {
            filter.unblockRequestOnBuckets(splitRange);
        }
        trace("Update global bucket table for buckets= from {} to {}", splitRange, fromShardId, toShardId);
        shardManagerClient.setBuckets(splitRange, fromShardId, toShardId, false);
    }

    @Override
    public boolean waitSlavesSynced(String shardId, long timeoutMs) throws ShardManagerException, InterruptedException {
        return waitLogApproach(shardId, timeoutMs, 0);
    }

    private boolean waitLogApproach(String shardId, long timeoutMs, int logPosDiff)
        throws ShardManagerException, InterruptedException {
        Shard shard = gondola.getShard(shardId);
        boolean complete = false;
        long start = System.currentTimeMillis();
        long now = start;
        long defaultSleep = timeoutMs / POLLING_TIMES;
        while (now - start < timeoutMs) {
            if (shard.getCommitIndex() != 0
                && shard.getCommitIndex() - getSavedIndex(shard) <= logPosDiff) {
                complete = true;
                break;
            }
            long remain = start + timeoutMs - System.currentTimeMillis();
            Thread.sleep(defaultSleep < remain ? defaultSleep : remain);
            now = System.currentTimeMillis();
        }
        return complete;
    }

    private int getSavedIndex(Shard shard) throws ShardManagerException {
        try {
            return shard.getLastSavedIndex();
        } catch (Exception e) {
            throw new ShardManagerException(e);
        }
    }

    @Override
    public boolean waitSlavesApproaching(String clusterId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        return waitLogApproach(clusterId, timeoutMs, LOG_APPROACHING_DIFF);
    }

    @Override
    public void setBuckets(Range<Integer> splitRange, String fromShardId, String toShardId, boolean migrationComplete) {
        trace("Update local bucket table: buckets={} => {} -> {}", splitRange, fromShardId, migrationComplete);
        filter.updateBucketRange(splitRange, fromShardId, toShardId, migrationComplete);
    }

    private void trace(String format, Object... args) {
        if (tracing) {
            logger.info(format, args);
        }
    }


}
