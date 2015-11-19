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

    public static final int RETRY_COUNT = 3;
    private String serviceName;
    private Config config;
    private ShardManagerClient shardManagerClient;

    Logger logger = LoggerFactory.getLogger(AdminClient.class);

    boolean tracing = false;

    /**
     * Instantiates a new Admin client.
     *
     * @param serviceName        the service name
     * @param shardManagerClient the shard manager client
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
    public void splitShard(String fromShardId, String toShardId) throws AdminException {
        Range<Integer> range = lookupSplitRange(fromShardId, toShardId);
        assignBuckets(fromShardId, toShardId, range);
    }

    /**
     * Assign buckets.
     *
     * @param fromShardId the from shard id
     * @param toShardId   the to shard id
     * @param range       the range
     */
    public void assignBuckets(String fromShardId, String toShardId, Range<Integer> range) {
        tracing("Executing assign buckets={} from {} to {}", range, fromShardId, toShardId);
        String step = "Before init";
        for (int i = 0; i < RETRY_COUNT; i++) {
            try {
                step = "initializing";
                tracing("Initializing slaves on {} ...", toShardId);
                for (Config.ConfigMember member : config.getMembersInShard(toShardId)) {
                    shardManagerClient.startObserving(member.getMemberId(), fromShardId);
                }

                step = "waiting for slave logs approaching";
                tracing("All nodes in {} are in slave mode sync'ing with the master's log.",
                        toShardId);
                shardManagerClient.waitApproaching(toShardId, -1);

                step = "assigning buckets";
                tracing("All nodes in {} logs approached to leader's log position, assigning buckets={} ...",
                        toShardId, range);
                // assignBucket is a atomic operation executing on leader at fromShard,
                // after operation is success, it will stop observing mode of toShard.
                for (Config.ConfigMember member : config.getMembersInShard(fromShardId)) {
                    shardManagerClient.assignBucket(member.getMemberId(), range, toShardId, 2000);
                }

                tracing("Assign buckets complete, assigned buckets={} from {} to {}", range, fromShardId, toShardId);
                step = "done";
                break;
            } catch (RuntimeException | ShardManagerProtocol.ShardManagerException e) {
                logger.warn("Error occurred in step {}.. retrying {} / {}", step, i, RETRY_COUNT, e);
            }
        }
    }

    private void tracing(String format, Object... args) {
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
    public void mergeShard(String fromShardId, String toShardId) throws AdminException {
        Range<Integer> range = lookupMergeRange(fromShardId, toShardId);
        assignBuckets(fromShardId, toShardId, range);
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
     * Enable tracing.
     *
     * @param target   the target
     * @param targetId the target id
     * @throws AdminException the admin exception
     */
    public void enableTracing(Target target, String targetId) throws AdminException {

    }


    /**
     * Disable tracing.
     *
     * @param target   the target
     * @param targetId the target id
     * @throws AdminException the admin exception
     */
    public void disableTracing(Target target, String targetId) throws AdminException {

    }

    /**
     * The enum Target.
     */
    enum Target {
        /**
         * Host target.
         */
        HOST, /**
         * Shard target.
         */
        SHARD, /**
         * Site target.
         */
        SITE, /**
         * Storage target.
         */
        STORAGE, /**
         * All target.
         */
        ALL
    }

    /**
     * The type Stat.
     */
    class Stat {

    }

    /**
     * The type Host stat.
     */
    class HostStat extends Stat {

    }

    /**
     * The type Storage stat.
     */
    class StorageStat extends Stat {

    }

    /**
     * The type Shard stat.
     */
    class ShardStat extends Stat {

    }

    /**
     * The type Admin exception.
     */
    class AdminException extends Exception {

        /**
         * The Error code.
         */
        ErrorCode errorCode;
    }

    /**
     * The enum Error code.
     */
    enum ErrorCode {
        /**
         * Config not found error code.
         */
        CONFIG_NOT_FOUND(10000);

        private int code;

        ErrorCode(int code) {
            this.code = code;
        }
    }
}
