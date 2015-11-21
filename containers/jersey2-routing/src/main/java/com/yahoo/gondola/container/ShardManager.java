/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Member;
import com.yahoo.gondola.Shard;
import com.yahoo.gondola.container.client.ShardManagerClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.yahoo.gondola.container.ShardManagerProtocol.ShardManagerException.CODE.FAILED_START_SLAVE;
import static com.yahoo.gondola.container.ShardManagerProtocol.ShardManagerException.CODE.FAILED_STOP_SLAVE;
import static com.yahoo.gondola.container.ShardManagerProtocol.ShardManagerException.CODE.NOT_LEADER;

/**
 * The Shard manager.
 */
public class ShardManager implements ShardManagerProtocol {

    // TODO: move to config
    public static final int SET_SLAVE_TIMEOUT_MS = 500;
    public static final int POLLING_TIMES = 5;
    public static final int LOG_APPROACHING_DIFF = 1000;

    Config config;
    RoutingFilter filter;

    ExecutorService executor = Executors.newSingleThreadExecutor();

    static Logger logger = LoggerFactory.getLogger(ShardManager.class);

    private Set<String> observedShards = new HashSet<>();

    private ShardManagerClient shardManagerClient;

    boolean tracing = false;

    public ShardManager(RoutingFilter filter, Config config, ShardManagerClient shardManagerClient) {
        this.filter = filter;
        this.config = config;
        this.shardManagerClient = shardManagerClient;
        this.config.registerForUpdates(config1 -> tracing = config1.getBoolean("tracing.router"));
    }


    /**
     * Starts observer mode to remote cluster.
     */
    @Override
    public void startObserving(String shardId, String observedShardId)
        throws ShardManagerException, InterruptedException {
        boolean success = false;
        trace("[{}] Connect to shardId={} as slave", shardId, observedShardId);
        for (Config.ConfigMember m : config.getMembersInShard(shardId)) {
            if (success = setSlave(shardId, m.getMemberId())) {
                break;
            }
        }
        if (!success) {
            throw new ShardManagerException(FAILED_START_SLAVE);
        }
        observedShards.add(observedShardId);
    }

    private boolean setSlave(String shardId, int memberId) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        filter.getGondola().getShard(shardId).getLocalMember().setSlave(memberId, slaveStatus -> latch.countDown());

        if (latch.await(SET_SLAVE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            trace("Timeout waiting for master memberId={}", memberId);
        }

        Member.SlaveStatus status = filter.getGondola().getShard(shardId).getLocalMember().getSlaveUpdate();

        if (status == null || !status.running) {
            logger.warn("Failed start observing {} on shard={}. msg={}", memberId, shardId,
                        status != null && status.exception != null ? status.exception.getMessage() : "");
            return false;
        }
        return true;
    }


    /**
     * Stops observer mode to remote cluster, and back to normal mode.
     */
    @Override
    public void stopObserving(String shardId, String observedShardId) throws ShardManagerException,
                                                                             InterruptedException {
        trace("[{}] Disconnect to shardId={} as slave", shardId, observedShardId);
        boolean success = false;
        for (Config.ConfigMember m : config.getMembersInShard(observedShardId)) {
            if (success = unsetSlave(shardId, m.getMemberId())) {
                break;
            }
        }
        if (!success) {
            throw new ShardManagerException(FAILED_STOP_SLAVE);
        }
        observedShards.remove(observedShardId);
    }

    private boolean unsetSlave(String shardId, int memberId) throws ShardManagerException, InterruptedException {
        Member.SlaveStatus slaveUpdate = filter.getGondola().getShard(shardId).getLocalMember().getSlaveUpdate();

        // Not in slave mode, nothing to do.
        if (slaveUpdate == null) {
            return true;
        }

        // Reject if following different leader
        if (slaveUpdate.memberId != memberId) {
            throw new ShardManagerException(FAILED_STOP_SLAVE,
                                            String.format(
                                                "Cannot stop slave due to different master. current=%d, target=%d",
                                                slaveUpdate.memberId, memberId));
        }

        CountDownLatch latch = new CountDownLatch(1);
        filter.getGondola().getShard(shardId).getLocalMember().setSlave(-1, update -> latch.countDown());
        latch.await();
        if (filter.getGondola().getShard(shardId).getLocalMember().getSlaveUpdate() != null) {
            throw new ShardManagerException(FAILED_STOP_SLAVE);
        }

        return true;
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
        MigrationType migrationType = getMigrationType(splitRange, toShardId);
        switch (migrationType) {
            case APP:
                try {
                    filter.blockRequestOnBuckets(splitRange);
                    waitNoRequestsOnBuckets(splitRange, timeoutMs);
                    shardManagerClient.waitSlavesSynced(toShardId, timeoutMs);
                    shardManagerClient.stopObserving(toShardId, fromShardId);
                    setBuckets(splitRange, fromShardId, toShardId, false);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    // TODO: rollback
                    logger.warn("Error occurred, rollback!", e);
                } finally {
                    filter.unblockRequestOnBuckets(splitRange);
                }
                trace("Update global bucket table for buckets= from {} to {}", splitRange, fromShardId, toShardId);
                shardManagerClient.setBuckets(splitRange, fromShardId, toShardId, false);
                break;
            case DB:
                // TODO: implement
        }
    }

    @Override
    public boolean waitSlavesSynced(String shardId, long timeoutMs) throws ShardManagerException, InterruptedException {
        return waitLogApproach(shardId, timeoutMs, 0);
    }

    private boolean waitLogApproach(String shardId, long timeoutMs, int logPosDiff)
        throws ShardManagerException, InterruptedException {
        Shard shard = filter.getGondola().getShard(shardId);
        boolean complete = false;
        long start = System.currentTimeMillis();
        long now = start;
        long defaultSleep = timeoutMs / POLLING_TIMES;
        while (now - start < timeoutMs) {
            if (shard.getCommitIndex() - getSavedIndex(shard) <= logPosDiff) {
                complete = true;
                break;
            }
            long remain = start + timeoutMs - System.currentTimeMillis();
            Thread.sleep(defaultSleep < remain ? remain : defaultSleep);
            now = System.currentTimeMillis();
        }
        return complete;
    }

    private int getSavedIndex(Shard shard) throws ShardManagerException{
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

    /**
     * Returns the migration type by inspect config, DB -> if two shards use different database APP -> if two shards use
     * same database.
     */
    private MigrationType getMigrationType(Range<Integer> splitRange, String toShard) {
        //TODO: implement
        return MigrationType.APP;
    }

    /**
     * Two types of migration APP -> Shared the same DB DB  -> DB migration.
     */
    enum MigrationType {
        APP,
        DB
    }

    private void trace(String format, Object... args) {
        if (tracing) {
            logger.info(format, args);
        }
    }

    private void waitNoRequestsOnBuckets(Range<Integer> splitRange, long timeoutMs)
        throws InterruptedException, ExecutionException, TimeoutException {
        // TODO: implement
        trace("Waiting for no requests on buckets: {} with timeout={}ms", splitRange, timeoutMs);
        executeTaskWithTimeout(() -> {
            if (splitRange != null) {
                //Thread.sleep(timeoutMs * 2);
            }
            return "";
        }, timeoutMs);
        trace("No more request on buckets: {}", splitRange);
    }

    private void executeTaskWithTimeout(Callable callable, long timeoutMs)
        throws InterruptedException, ExecutionException, TimeoutException {
        executor.submit(callable)
            .get(timeoutMs, TimeUnit.MILLISECONDS);
    }
}
