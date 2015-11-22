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

import java.util.List;
import java.util.Map;

/**
 * The type Admin client.
 */
public class AdminClient {

    // TODO: move to config
    private static final int RETRY_COUNT = 3;
    public static final int TIMEOUT_MS = 300;
    private String serviceName;
    private Config config;
    private ShardManagerClient shardManagerClient;

    private static Logger logger = LoggerFactory.getLogger(AdminClient.class);
    private boolean tracing = false;

    /**
     * Instantiates a new Admin client.
     *
     * @param serviceName        the service name
     * @param shardManagerClient the shard manager client
     * @param config             the config
     */
    public AdminClient(String serviceName, ShardManagerClient shardManagerClient, Config config) {
        this.serviceName = serviceName;
        this.shardManagerClient = shardManagerClient;
        this.config = config;
        this.config.registerForUpdates(config1 -> {
            tracing = config1.getBoolean("tracing.adminCli");
        });
    }


    /**
     * Sets service name.
     *
     * @param serviceName the service name
     * @throws AdminException the admin exception
     */
    public void setServiceName(String serviceName) throws AdminException {
        this.serviceName = serviceName;
    }


    /**
     * Gets service name.
     *
     * @return the service name
     * @throws AdminException the admin exception
     */
    public String getServiceName() throws AdminException {
        return serviceName;
    }


    /**
     * Gets config.
     *
     * @return the config
     * @throws AdminException the admin exception
     */
    public Config getConfig() throws AdminException {
        return config;
    }


    /**
     * Sets config.
     *
     * @param config the config
     * @throws AdminException the admin exception
     */
    public void setConfig(Config config) throws AdminException {
        this.config = config;
    }


    /**
     * Split shard.
     *
     * @param fromShardId the from shard id
     * @param toShardId   the to shard id
     * @throws AdminException the admin exception
     */
    public void splitShard(String fromShardId, String toShardId) throws AdminException, InterruptedException {
        Range<Integer> range = lookupSplitRange(fromShardId, toShardId);
        assignBuckets(range, fromShardId, toShardId);
    }

    /**
     * Assign buckets.
     *
     * @param fromShardId the from shard id
     * @param toShardId   the to shard id
     * @param range       the range
     */
    public void assignBuckets(Range<Integer> range, String fromShardId, String toShardId)
        throws InterruptedException, AdminException {
        trace("Executing assign buckets={} from {} to {}", range, fromShardId, toShardId);
        String step = "Before init";
        boolean complete = false;
        for (int i = 1; i <= RETRY_COUNT; i++) {
            try {
                step = "initializing";
                trace("Initializing slaves on {} ...", toShardId);
                shardManagerClient.startObserving(fromShardId, toShardId, TIMEOUT_MS);

                step = "waiting for slave logs approaching";
                trace(
                    "All nodes in {} are in slave mode, waiting for slave logs approaching to leader's log position.",
                    toShardId);

                if (!shardManagerClient.waitSlavesApproaching(toShardId, -1)) {
                    break;
                }

                step = "assigning buckets";
                trace("All nodes in {} logs approached to leader's log position, assigning buckets={} ...", toShardId,
                      range);
                // migrateBuckets is a atomic operation executing on leader at fromShard,
                // after operation is success, it will stop observing mode of toShard.
                shardManagerClient.migrateBuckets(range, fromShardId, toShardId, 2000);

                trace("Assign buckets complete, assigned buckets={} from {} to {}", range, fromShardId, toShardId);
                step = "done";
                complete = true;
                break;
            } catch (ShardManagerProtocol.ShardManagerException e) {
                if (i != RETRY_COUNT) {
                    logger.warn("Error occurred in step {}.. retrying {} / {}, errorMsg={}",
                                step, i, RETRY_COUNT, e.getMessage());
                } else {
                    logger.error("Assign bucket failed, lastError={}", e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }
        if (!complete) {
            throw new AdminException();
        }
    }

    /**
     * Close assign buckets.
     *
     * @param fromShardId the from shard id
     * @param toShardId   the to shard id
     * @param range       the range
     */
    public void closeAssignBuckets(Range<Integer> range, String fromShardId, String toShardId)
        throws ShardManagerProtocol.ShardManagerException {
        // TODO: implement
        trace("Executing close the state of assign buckets");
        trace("Waiting all nodes bucket table updated...");
        // waiting..
        trace("closing the state of assign buckets...");
        shardManagerClient.setBuckets(range, fromShardId, toShardId, true);
        trace("Done!");
    }

    private void trace(String format, Object... args) {
        if (tracing) {
            logger.info(format, args);
        }
    }

    private Range<Integer> lookupSplitRange(String fromShardId, String toShardId) {
        // TODO: implement
        return Range.closed(1, 5);
    }


    /**
     * Merge shard.
     *
     * @param fromShardId the from shard id
     * @param toShardId   the to shard id
     * @throws AdminException the admin exception
     */
    public void mergeShard(String fromShardId, String toShardId) throws AdminException, InterruptedException {
        Range<Integer> range = lookupMergeRange(fromShardId, toShardId);
        assignBuckets(range, fromShardId, toShardId);
    }

    private Range<Integer> lookupMergeRange(String fromShardId, String toShardId) {
        // TODO: implement
        return Range.closed(1, 5);
    }


    /**
     * Enable.
     *
     * @param target   the target
     * @param targetId the target id
     * @throws AdminException the admin exception
     */
    public void enable(Target target, String targetId) throws AdminException {

    }


    /**
     * Disable.
     *
     * @param target   the target
     * @param targetId the target id
     * @throws AdminException the admin exception
     */
    public void disable(Target target, String targetId) throws AdminException {

    }


    /**
     * Gets stats.
     *
     * @param target the target
     * @return the stats
     * @throws AdminException the admin exception
     */
    public Map<Target, List<Stat>> getStats(Target target)
        throws AdminException {
        return null;
    }


    /**
     * Enable trace.
     *
     * @param target   the target
     * @param targetId the target id
     * @throws AdminException the admin exception
     */
    public void enableTracing(Target target, String targetId) throws AdminException {

    }


    /**
     * Disable trace.
     *
     * @param target   the target
     * @param targetId the target id
     * @throws AdminException the admin exception
     */
    public void disableTracing(Target target, String targetId) throws AdminException {

    }

    enum Target {
        HOST,
        SHARD,
        SITE,
        STORAGE,
        ALL
    }

    class Stat {

    }

    class HostStat extends Stat {

    }

    class StorageStat extends Stat {

    }

    class ShardStat extends Stat {

    }

    class AdminException extends Exception {
        ErrorCode errorCode;
    }

    enum ErrorCode {
        CONFIG_NOT_FOUND(10000);
        private int code;
        ErrorCode(int code) {
            this.code = code;
        }
    }
}
