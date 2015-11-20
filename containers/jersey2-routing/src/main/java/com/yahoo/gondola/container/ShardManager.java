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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.yahoo.gondola.container.ShardManagerProtocol.ShardManagerException.CODE.NOT_LEADER;

/**
 * The Shard manager.
 */
public class ShardManager implements ShardManagerProtocol {

    Config config;
    RoutingFilter filter;

    ExecutorService executor = Executors.newSingleThreadExecutor();

    static Logger logger = LoggerFactory.getLogger(ShardManager.class);

    private Set<String> observedShards = new HashSet<>();

    public Set<String> getObservedShards() {
        return observedShards;
    }

    private ShardManagerClient shardManagerClient;

    boolean tracing = false;

    public ShardManager(RoutingFilter filter, Config config, ShardManagerClient shardManagerClient) {
        this.filter = filter;
        this.config = config;
        this.shardManagerClient = shardManagerClient;
        this.config.registerForUpdates(config1 -> {
            tracing = config1.getBoolean("tracing.router");
        });
    }


    /**
     * Starts observer mode to remote cluster.
     */
    @Override
    public void startObserving(String shardId, String observedShardId) throws ShardManagerException {
        // TODO: gondola start observing
        if (tracing) {
            logger.info("[{}] Connect to shardId={} as slave", shardId, observedShardId);
        }
        observedShards.add(observedShardId);
    }


    /**
     * Stops observer mode to remote cluster, and back to normal mode.
     */
    @Override
    public void stopObserving(String shardId, String observedShardId) throws ShardManagerException {
        // TODO: gondola stop observing
        if (tracing) {
            logger.info("[{}] Disconnect to shardId={} as slave", shardId, observedShardId);
        }
        observedShards.remove(observedShardId);
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
    public boolean waitSlavesSynced(String shardId, long timeoutMs) throws ShardManagerException {
        return true;
    }

    @Override
    public boolean waitApproaching(String clusterId, long timeoutMs) throws ShardManagerException {
        return true;
    }

    @Override
    public void setBuckets(Range<Integer> splitRange, String fromShardId, String toShardId, boolean migrationComplete)
        throws ShardManagerException {
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

    void waitNoRequestsOnBuckets(Range<Integer> splitRange, long timeoutMs)
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

    void executeTaskWithTimeout(Callable callable, long timeoutMs)
        throws InterruptedException, ExecutionException, TimeoutException {
        executor.submit(callable)
            .get(timeoutMs, TimeUnit.MILLISECONDS);
    }
}
