package com.yahoo.gondola.container;

import com.yahoo.gondola.Config;

import java.util.List;
import java.util.Map;

public interface AdminInterface {

    void setServiceName(String serviceName) throws AdminException;

    String getServiceName() throws AdminException;

    Config getConfig() throws AdminException;

    void setConfig(Config config) throws AdminException;

    void splitShard(String fromShardId, String toShardId) throws AdminException;

    void mergeShard(String fromShardId, String toShardId) throws AdminException;

    void enable(Target target, String targetId) throws AdminException;

    void disable(Target target, String targetId) throws AdminException;

    Map<Target, List<Stat>> getStats(Target target) throws AdminException;

    void enableTracing(Target target, String targetId) throws AdminException;

    void disableTracing(Target target, String targetId) throws AdminException;

    enum Target {
        HOST, SHARD, SITE, STORAGE, ALL
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
