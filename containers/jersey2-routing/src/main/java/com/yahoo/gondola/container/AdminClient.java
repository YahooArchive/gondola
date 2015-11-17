package com.yahoo.gondola.container;

import com.yahoo.gondola.Config;

import java.util.List;
import java.util.Map;

public class AdminClient implements AdminInterface {

    private String serviceName;
    private Config config;
    private ShardManager shardManager;


    public AdminClient(String serviceName, ShardManager shardManager) {
        this.serviceName = serviceName;
        this.shardManager = shardManager;
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



    }

    @Override
    public void mergeShard(String fromShardId, String toShardId) throws AdminException {

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
