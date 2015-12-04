/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.Gondola;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Manages lock activity.
 */
class LockManager {

    private static Logger logger = LoggerFactory.getLogger(LockManager.class);
    private CountDownLatch globalLock;
    private Map<String, CountDownLatch> shardLocks = new ConcurrentHashMap<>();
    private Map<Range<Integer>, CountDownLatch> bucketLocks = new HashMap<>();
    private ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private boolean tracing = false;
    private Gondola gondola;

    /**
     * Instantiates a new Lock manager.
     *
     * @param gondola The Gondola instance.
     */
    public LockManager(Gondola gondola) {
        gondola.getConfig().registerForUpdates(config1 -> tracing = config1.getBoolean("tracing.router"));
        this.gondola = gondola;
    }

    /**
     * Filter request.
     *
     * @param bucketId the bucket id
     * @param shardId  the shard id
     * @throws InterruptedException the interrupted exception
     */
    public void filterRequest(int bucketId, String shardId) throws InterruptedException {
        if (globalLock != null) {
            trace("[{}] Request blocked by global lock", gondola.getHostId());
            globalLock.await();
        }

        CountDownLatch shardLock = shardLocks.get(shardId);
        if (shardLock != null) {
            trace("[{}] Request blocked by shard lock - shardId={}", gondola.getHostId(), shardId);
            shardLock.await();
        }

        List<CountDownLatch> bucketLocks = getBucketLocks(bucketId);
        if (bucketLocks.size() != 0) {
            for (CountDownLatch bucketLock : bucketLocks) {
                trace("[{}] Request blocked by bucket lock - bucketId={}", gondola.getHostId(), bucketId);
                bucketLock.await();
            }
        }
    }

    /**
     * Unblock request on shard long.
     *
     * @param shardId the shard id
     * @return the long
     */
    public long unblockRequestOnShard(String shardId) {
        CountDownLatch lock = shardLocks.remove(shardId);
        if (lock != null) {
            // TODO: this is not the expected blocking count.
            long count = lock.getCount();
            lock.countDown();
            trace("[{}] Request unblocked on shardId={}", gondola.getHostId(), shardId);
            return count;
        }
        return 0;
    }

    /**
     * Block request on shard.
     *
     * @param shardId the shard id
     */
    public void blockRequestOnShard(String shardId) {
        trace("[{}] Block requests on shard : {}", gondola.getHostId(), shardId);
        shardLocks.putIfAbsent(shardId, new CountDownLatch(1));
    }

    /**
     * Unblock all requests.
     */
    public void unblockRequest() {
        trace("[{}] Unblock all requests", gondola.getHostId());
        if (globalLock != null) {
            globalLock.countDown();
            globalLock = null;
        }
    }

    /**
     * Block all requests.
     */
    public void blockRequest() {
        trace("[{}] Block all requests", gondola.getHostId());
        globalLock = new CountDownLatch(1);
    }

    /**
     * Unblock request on buckets.
     *
     * @param splitRange the split range
     */
    public void unblockRequestOnBuckets(Range<Integer> splitRange) {
        trace("[{}] Unblock requests on buckets : {}", gondola.getHostId(), splitRange);
        CountDownLatch lock = bucketLocks.remove(splitRange);
        if (lock != null) {
            lock.countDown();
        }
    }


    /**
     * Block request on buckets.
     *
     * @param splitRange the split range
     */
    public void blockRequestOnBuckets(Range<Integer> splitRange) {
        trace("[{}] Block requests on buckets : {}", gondola.getHostId(), splitRange);
        bucketLocks.putIfAbsent(splitRange, new CountDownLatch(1));
    }

    private List<CountDownLatch> getBucketLocks(int bucketId) throws InterruptedException {
        rwLock.readLock().lock();
        try {
            return bucketLocks.entrySet().stream()
                .filter(e -> e.getKey().contains(bucketId))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void trace(String format, Object... args) {
        if (tracing) {
            logger.info(format, args);
        }
    }

    public CountDownLatch getGlobalLock() {
        return globalLock;
    }

    public Map<String, CountDownLatch> getShardLocks() {
        return shardLocks;
    }

    public Map<Range<Integer>, CountDownLatch> getBucketLocks() {
        return bucketLocks;
    }
}
