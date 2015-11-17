/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.client.ShardManagerClient;

import java.util.List;
import java.util.Map;

/**
 * The type Admin client.
 */
public class AdminClient {

    private String serviceName;
    private Config config;
    private ShardManagerClient shardManagerClient;

    /**
     * Instantiates a new Admin client.
     *
     * @param serviceName        the service name
     * @param shardManagerClient the shard manager client
     */
    public AdminClient(String serviceName, ShardManagerClient shardManagerClient) {
        this.serviceName = serviceName;
        this.shardManagerClient = shardManagerClient;
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

    private void assignBuckets(String fromShardId, String toShardId, Range<Integer> range) {
        shardManagerClient.allowObserver(fromShardId, toShardId);
        for (int i = 0; i < 3; i++) {
            try {
                shardManagerClient.startObserving(toShardId, fromShardId);
                // assignBucket is a atomic operation executing on leader at fromShard,
                // after operation is success, it will stop observing mode of toShard.
                shardManagerClient.assignBucket(fromShardId, range, toShardId, 2000);
                shardManagerClient.disallowObserver(fromShardId, toShardId);
            } catch (RuntimeException e) {
                // TODO: implement
            }
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
