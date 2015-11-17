package com.yahoo.gondola.container;

import com.google.common.collect.Range;

public interface ShardManagerProtocol {

    void allowObserver(String shardId, String allowedShardId);

    void disallowObserver(String shardId, String allowedShardId);

    void startObserving(String shardId, String observedShardId);

    void stopObserving(String shardId, String observedShardId);

    void assignBucket(String shardId, Range<Integer> splitRange, String toShardId, long timeoutMs);
}
