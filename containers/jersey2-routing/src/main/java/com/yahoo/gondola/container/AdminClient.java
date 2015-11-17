package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.client.ShardManagerClient;
import com.yahoo.gondola.container.client.StatClient;

import java.util.List;
import java.util.Map;

public class AdminClient implements AdminInterface {

    private String serviceName;
    private Config config;
    private ShardManagerClient shardManagerClient;
    private StatClient statClient;


    public AdminClient(String serviceName, ShardManagerClient shardManagerClient, StatClient statClient) {
        this.serviceName = serviceName;
        this.shardManagerClient = shardManagerClient;
        this.statClient = statClient;
    }

    @Override
    public void setServiceName(String serviceName) throws AdminException {
        this.serviceName = serviceName;
    }

    @Override
    public String getServiceName() throws AdminException {
        return serviceName;
    }

    @Override
    public Config getConfig() throws AdminException {
        return config;
    }

    @Override
    public void setConfig(Config config) throws AdminException {
        this.config = config;
    }

    @Override
    public void splitShard(String fromShardId, String toShardId) throws AdminException {
        Range<Integer> range = lookupSplitRange(fromShardId, toShardId);
        assignBuckets(fromShardId, toShardId, range);
    }

    private void assignBuckets(String fromShardId, String toShardId, Range<Integer> range) {
        shardManagerClient.allowObserver(fromShardId, toShardId);
        for (int i = 0; i < 3 ; i++) {
            try {
                shardManagerClient.startObserving(toShardId, fromShardId);
                // assignBucket is a atomic operation executing on leader at fromShard,
                // after operation is success, it will stop observing mode of toShard.
                shardManagerClient.assignBucket(fromShardId, range, toShardId, 2000);
                shardManagerClient.disallowObserver(fromShardId, toShardId);
            } catch (RuntimeException e) {
            }
        }
    }

    private Range<Integer> lookupSplitRange(String fromShardId, String toShardId) {
        // TODO: implement
        return Range.closed(1, 5);
    }

    @Override
    public void mergeShard(String fromShardId, String toShardId) throws AdminException {
        Range<Integer> range = lookupMergeRange(fromShardId, toShardId);
        assignBuckets(fromShardId, toShardId, range);
    }

    private Range<Integer> lookupMergeRange(String fromShardId, String toShardId) {
        // TODO: implement
        return Range.closed(1,5);
    }

    @Override
    public void enable(Target target, String targetId) throws AdminException {

    }

    @Override
    public void disable(Target target, String targetId) throws AdminException {

    }

    @Override
    public Map<Target, List<Stat>> getStats(Target target)
        throws AdminException {
        return null;
    }

    @Override
    public void enableTracing(Target target, String targetId) throws AdminException {

    }

    @Override
    public void disableTracing(Target target, String targetId) throws AdminException {

    }
}
