/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.ShardManagerProtocol.ShardManagerException;
import com.yahoo.gondola.container.client.ShardManagerClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

import static com.yahoo.gondola.container.ShardManagerProtocol.ShardManagerException.CODE.SLAVE_NOT_SYNC;

/**
 * The type Admin client.
 */
public class AdminClient {

    // TODO: move to config
    public static final int RETRY_COUNT = 3;
    public static final int TIMEOUT_MS = 3000;
    private String serviceName;
    private Config config;
    private ShardManagerClient shardManagerClient;
    private ConfigWriter configWriter;
    private GondolaAdminClient gondolaAdminClient;


    private static Logger logger = LoggerFactory.getLogger(AdminClient.class);
    private boolean tracing = false;

    /**
     * Instantiates a new Admin client.
     *
     * @param serviceName        the service name
     * @param shardManagerClient the shard manager client
     * @param config             the config
     */
    public AdminClient(String serviceName, ShardManagerClient shardManagerClient, Config config,
                       ConfigWriter configWriter, GondolaAdminClient gondolaAdminClient) {
        this.serviceName = serviceName;
        this.shardManagerClient = shardManagerClient;
        this.config = config;
        this.configWriter = configWriter;
        this.config.registerForUpdates(config1 -> {
            tracing = config1.getBoolean("tracing.adminCli");
        });
        this.gondolaAdminClient = gondolaAdminClient;
    }

    public static AdminClient getInstance(Config config, String clientName) {
        return new AdminClient(Utils.getRegistryConfig(config).attributes.get("serviceName"),
                               new ShardManagerProvider().getShardManagerClient(config, clientName),
                               config,
                               new ConfigWriter(config.getFile()),
                               new GondolaAdminClient(config));
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
     * @param configFile the config file.
     * @throws AdminException the admin exception
     */
    public void setConfig(File configFile) throws AdminException {
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
        assignBuckets(range.lowerEndpoint(), range.upperEndpoint(), fromShardId, toShardId);
    }

    /**
     * Assign buckets.
     */
    public void assignBuckets(int lowerBound, int upperBound, String fromShardId, String toShardId)
        throws InterruptedException, AdminException {
        Range range = Range.closed(lowerBound, upperBound);
        trace("[admin] Executing assign buckets={} from {} to {}", range, fromShardId, toShardId);
        for (int i = 1; i <= RETRY_COUNT; i++) {
            try {
                trace("[admin] Initializing slaves on {} ...", toShardId);
                shardManagerClient.startObserving(toShardId, fromShardId, TIMEOUT_MS * 3);

                trace(
                    "[admin] All nodes in {} are in slave mode, "
                    + "waiting for slave logs approaching to leader's log position.",
                    toShardId);

                if (!shardManagerClient.waitSlavesApproaching(toShardId, -1)) {
                    throw new ShardManagerException(SLAVE_NOT_SYNC);
                }

                trace("[admin] All nodes in {} logs approached to leader's log position, assigning buckets={} ...",
                      toShardId, range);
                // migrateBuckets is a atomic operation executing on leader at fromShard,
                // after operation is success, it will stop observing mode of toShard.
                shardManagerClient.migrateBuckets(range, fromShardId, toShardId, TIMEOUT_MS);
                trace("[admin] success!");
                trace("[admin] Writing latest config to config storage!");
                saveConfig(fromShardId, toShardId);
                break;
            } catch (ShardManagerException e) {
                logger.warn("Error occurred during assign buckets.. retrying {} / {}, errorMsg={}",
                            i, RETRY_COUNT, e.getMessage());
                try {
                    shardManagerClient.stopObserving(toShardId, fromShardId, TIMEOUT_MS);
                    shardManagerClient.rollbackBuckets(range);
                } catch (ShardManagerException e1) {
                    logger.info("Rollback, Stop observing failed, ignoring the error. msg={}", e1.getMessage());
                }
                if (i == RETRY_COUNT) {
                    logger.error("Assign bucket failed, lastError={}", e.getMessage());
                    throw new AdminException(e);
                }
            }
        }
    }

    private void saveConfig(String fromShardId, String toShardId) throws AdminException {
        configWriter.setBucketMap(fromShardId, getBucketMapString(fromShardId));
        configWriter.setBucketMap(fromShardId, getBucketMapString(toShardId));
        setConfig(configWriter.save());
    }

    private String getBucketMapString(String fromShardId) {
        // TODO: implement
        return "";
    }

    private void trace(String format, Object... args) {
        if (tracing) {
            logger.info(format, args);
        }
    }

    private Range<Integer> lookupSplitRange(String fromShardId, String toShardId) {
        // TODO: implement
        return Range.closed(0, 5);
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
        assignBuckets(range.lowerEndpoint(), range.upperEndpoint(), fromShardId, toShardId);
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
     * @param enable   enable or disable
     * @throws AdminException the admin exception
     */
    public void enable(Target target, String targetId, boolean enable) throws AdminException {
        switch (target) {
            case HOST:
                for (String shardId : config.getShardIds(targetId)) {
                    gondolaAdminClient.enable(targetId, shardId, enable);
                }
                break;
            case SHARD:
                throw new UnsupportedOperationException("Enable/disable a shard is not allowed.");
            case SITE:
                config.getHostIds().stream().filter(hostId -> config.getSiteIdForHost(hostId).equals(targetId)).forEach(
                    hostId -> {
                        for (String shardId : config.getShardIds(hostId)) {
                            gondolaAdminClient.enable(hostId, shardId, enable);
                        }
                    }
                );
                break;
            case STORAGE:
                break;
            case ALL:
                throw new UnsupportedOperationException("Enable/disable a shard is not allowed.");
            default:
                throw new IllegalStateException("unsupported target");
        }
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

    class AdminException extends Exception {

        ErrorCode errorCode;


        public AdminException(ShardManagerException e) {
            super(e);
        }

        public AdminException() {

        }
    }

    enum ErrorCode {
        CONFIG_NOT_FOUND(10000);
        private int code;

        ErrorCode(int code) {
            this.code = code;
        }
    }

    public Map setLeader(String hostId, String shardId) {
        return gondolaAdminClient.setLeader(hostId, shardId);
    }

    public Map getHostStatus(String hostId) {
        return gondolaAdminClient.getHostStatus(hostId);
    }

    public Map<String, Object> getServiceStatus() {
        return gondolaAdminClient.getServiceStatus();
    }

    public Map inspectRequestUri(String hostId, String uri) {
        return gondolaAdminClient.inspectRequestUri(uri, hostId);
    }
}
