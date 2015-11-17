/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.container.client.StatClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

/**
 * The Shard manager
 */
public class ShardManager {

    public static final int RETRY = 3;

    RoutingFilter filter;
    /**
     * The Stat client.
     */
    StatClient statClient;

    static Logger logger = LoggerFactory.getLogger(ShardManager.class);

    private Set<String> observedShards = new HashSet<>();

    public Set<String> getObservedShards() {
        return observedShards;
    }

    public Set<Integer> getAllowedObservers() {
        return allowedObservers;
    }

    private Set<Integer> allowedObservers = new HashSet<>();

    public ShardManager(RoutingFilter filter, StatClient statClient) {
        this.filter = filter;
        this.statClient = statClient;
    }


    /**
     * Enables special mode on gondola to allow observer.
     */
    public void allowObserver(int memberId) {
        // TODO: gondola allow observer
        logger.info("allow observer connect from memberId={}", memberId);
        allowedObservers.add(memberId);
    }


    /**
     * Disables special mode on gondola to allow observer.
     */
    public void disallowObserver(int memberId) {
        // TODO: gondola allow observer
        logger.info("disallow observer connect from memberId={}", memberId);
        allowedObservers.remove(memberId);
    }

    /**
     * Starts observer mode to remote cluster.
     */
    public void startObserving(String shardId) {
        // TODO: gondola start observing
        logger.info("start observer to shardId={}", shardId);
        observedShards.add(shardId);
    }


    /**
     * Stops observer mode to remote cluster, and back to normal mode.
     */
    public void stopObserving(String shardId) {
        // TODO: gondola stop observing
        logger.info("stop observer to shardId={}", shardId);
        observedShards.remove(shardId);
    }

    ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    /**
     * Splits the bucket of fromCluster, and reassign the buckets to toCluster.
     */
    public void assignBucket (Range<Integer> splitRange, String toShard, long timeoutMs) {
        MigrationType migrationType = getMigrationType(splitRange, toShard);
        switch (migrationType) {
            case APP:
                for (int i = 0; i < RETRY; i++) {
                    try {
                        filter.getLockManager().blockRequestOnBuckets(splitRange);
                        filter.waitNoRequestsOnBuckets(splitRange, timeoutMs);
                        filter.reassignBuckets(splitRange, toShard, timeoutMs);
                        break;
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        // TODO: rollback
                    } finally {
                        filter.getLockManager().unblockRequestOnBuckets(splitRange);
                    }
                }
                break;
            case DB:
                for (int i = 0; i < RETRY; i++) {
                    try {
                        statClient.waitApproaching(toShard, -1L);
                        filter.getLockManager().blockRequest();
                        statClient.waitSynced(toShard, 5000L);
                        filter.reassignBuckets(splitRange, toShard, timeoutMs);
                        break;
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        // TODO: rollback
                    } finally {
                        filter.getLockManager().unblockRequest();
                    }
                }
                break;
        }
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
     * Two types of migration APP -> Shared the same DB DB  -> DB migration
     */
    enum MigrationType {
        APP,
        DB
    }
}
