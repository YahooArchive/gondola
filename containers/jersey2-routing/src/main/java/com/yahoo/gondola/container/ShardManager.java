/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.GondolaException;
import com.yahoo.gondola.Member;
import com.yahoo.gondola.Shard;
import com.yahoo.gondola.container.client.ShardManagerClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.yahoo.gondola.container.ShardManagerProtocol.ShardManagerException.CODE.FAILED_START_SLAVE;
import static com.yahoo.gondola.container.ShardManagerProtocol.ShardManagerException.CODE.FAILED_STOP_SLAVE;
import static com.yahoo.gondola.container.ShardManagerProtocol.ShardManagerException.CODE.MASTER_IS_GONE;

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
     * Starts observer mode to remote shard.
     */
    @Override
    public void startObserving(String shardId, String observedShardId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        boolean success = false;
        trace("[{}-{}] Try to follow shardId={} as slave...",
              gondola.getHostId(), gondola.getShard(shardId).getLocalMember().getMemberId(), observedShardId);
        List<Config.ConfigMember> membersInShard = config.getMembersInShard(observedShardId);
        for (Config.ConfigMember m : membersInShard) {
            if (success = setSlave(shardId, m.getMemberId(), timeoutMs)) {
                filter.getChangeLogProcessor().reset(shardId);
                trace("[{}-{}] Successfully to follow masterId={}",
                      gondola.getHostId(), gondola.getShard(shardId).getLocalMember().getMemberId(), m.getMemberId());
                break;
            }
        }
        if (!success) {
            logger.error("[{}-{}] Failed follow master={}",
                         gondola.getHostId(), gondola.getShard(shardId).getLocalMember().getMemberId(),
                         observedShardId);
            throw new ShardManagerException(FAILED_START_SLAVE);
        }
        observedShards.add(observedShardId);
    }

    private boolean setSlave(String shardId, int memberId, long timeoutMs)
        throws InterruptedException, ShardManagerException {

        try {
            Member localMember = gondola.getShard(shardId).getLocalMember();
            Member.SlaveStatus slaveStatus = localMember.getSlaveStatus();
            if (slaveStatus != null && slaveStatus.masterId == memberId && slaveStatus.running) {
                return true;
            }
            localMember.setSlave(memberId);
            return Utils.pollingWithTimeout(() -> {
                Member.SlaveStatus status = gondola.getShard(shardId).getLocalMember().getSlaveStatus();
                if (slaveOperational(status)) {
                    trace("[{}] Successfully connect to leader node={}", gondola.getHostId(), memberId);
                    return true;
                }
                trace("[{}] Slave status={} role={}", gondola.getHostId(), status, gondola.getShard(shardId).getLocalRole());
                return false;
            }, timeoutMs / POLLING_TIMES, timeoutMs);
        } catch (Exception e) {
            throw new ShardManagerException(e);
        }
    }

    private boolean slaveOperational(Member.SlaveStatus status) {
        return status != null && status.running;
    }


    /**
     * Stops observer mode to remote shard, and back to normal mode.
     */
    @Override
    public void stopObserving(String shardId, String masterShardId, long timeoutMs) throws ShardManagerException,
                                                                                           InterruptedException {
        trace("[{}] shardId={} un-followed shardId={}", gondola.getHostId(), shardId, masterShardId);
        Member.SlaveStatus status = gondola.getShard(shardId).getLocalMember().getSlaveStatus();
        if (status == null) {
            return;
        }

        String curMasterShardId = config.getMember(status.masterId).getShardId();

        if (!curMasterShardId.equals(masterShardId)) {
            throw
                new ShardManagerException(FAILED_STOP_SLAVE,
                                          String.format(
                                              "Cannot stop slave due to follow different shard. current=%s, target=%s",
                                              curMasterShardId, masterShardId));
        }

        try {
            gondola.getShard(shardId).getLocalMember().setSlave(-1);
        } catch (GondolaException e) {
            throw new ShardManagerException(e);
        }

        try {
            if (!Utils.pollingWithTimeout(() -> {
                if (gondola.getShard(shardId).getLocalMember().getSlaveStatus() == null) {
                    return true;
                }
                logger.warn("Failed stop observing {} on shard={}", masterShardId, shardId);
                return false;
            }, timeoutMs / POLLING_TIMES, timeoutMs)) {
                throw new ShardManagerException(FAILED_STOP_SLAVE, "timed out");
            }
        } catch (ExecutionException e) {
            throw new ShardManagerException(e);
        }
        observedShards.remove(masterShardId);
    }

    /**
     * Splits the bucket of fromShard, and reassign the buckets to toShard.
     */
    @Override
    public void migrateBuckets(Range<Integer> splitRange, String fromShardId,
                               String toShardId, long timeoutMs) throws ShardManagerException {
        // Make sure only leader can execute this request.
        if (!filter.isLeaderInShard(fromShardId)) {
            return;
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
            filter.updateBucketRange(splitRange, fromShardId, toShardId, false);
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Error occurred, rollback!", e);
            try {
                shardManagerClient.startObserving(fromShardId, toShardId, timeoutMs);
            } catch (InterruptedException e1) {
                logger.warn("Cannot start observing while performing rollback operation. msg={}", e1.getMessage());
            }
        } finally {
            filter.unblockRequestOnBuckets(splitRange);
        }
    }

    @Override
    public boolean waitSlavesSynced(String shardId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        return waitLogApproach(shardId, timeoutMs, 0);
    }

    private boolean waitLogApproach(String shardId, long timeoutMs, int logPosDiff)
        throws ShardManagerException, InterruptedException {
        Shard shard = gondola.getShard(shardId);
        try {
            return Utils.pollingWithTimeout(() -> {
                if (shard.getCommitIndex() != 0 && shard.getCommitIndex() - getSavedIndex(shard) <= logPosDiff) {
                    return true;
                }
                Member.SlaveStatus slaveStatus = shard.getLocalMember().getSlaveStatus();
                if (!slaveOperational(slaveStatus)) {
                    throw new ShardManagerException(MASTER_IS_GONE);
                }
                trace("[{}] {} Log status={}, currentDiff={}, targetDiff={}",
                      gondola.getHostId(), shardId, slaveOperational(slaveStatus) ? "RUNNING" : "DOWN",
                      slaveOperational(slaveStatus) ? "N/A" : shard.getCommitIndex() - getSavedIndex(shard),
                      logPosDiff);
                return false;
            }, timeoutMs / POLLING_TIMES, timeoutMs);
        } catch (ExecutionException e) {
            throw new ShardManagerException(e);
        }
    }

    private int getSavedIndex(Shard shard) throws ShardManagerException {
        try {
            return shard.getLastSavedIndex();
        } catch (Exception e) {
            throw new ShardManagerException(e);
        }
    }

    @Override
    public boolean waitSlavesApproaching(String shardId, long timeoutMs)
        throws ShardManagerException, InterruptedException {
        return waitLogApproach(shardId, timeoutMs, LOG_APPROACHING_DIFF);
    }

    @Override
    public void setBuckets(Range<Integer> splitRange, String fromShardId, String toShardId, boolean migrationComplete) {
        trace("[{}] Update local bucket table: buckets={} {} => {}. status={}",
              gondola.getHostId(), splitRange, fromShardId, toShardId, migrationComplete ? "COMPLETE" : "MIGRATING");
        filter.getBucketManager().updateBucketRange(splitRange, fromShardId, toShardId, migrationComplete);
    }

    @Override
    public void rollbackBuckets(Range<Integer> splitRange) {
        filter.getBucketManager().rollbackBuckets(splitRange);
    }

    private void trace(String format, Object... args) {
        if (tracing) {
            logger.info(format, args);
        }
    }

    public ShardManagerClient getShardManagerClient() {
        return shardManagerClient;
    }
}
