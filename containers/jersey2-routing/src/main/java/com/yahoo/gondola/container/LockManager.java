/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;

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
 * Manages lock activity
 */
class LockManager {
    static Logger logger = LoggerFactory.getLogger(LockManager.class);

    Config config;
    CountDownLatch globalLock;
    Map<String, CountDownLatch> shards = new ConcurrentHashMap<>();
    Map<Range<Integer>, CountDownLatch> buckets = new HashMap<>();
    ReadWriteLock rwLock = new ReentrantReadWriteLock();
    boolean tracing = false;

    public LockManager(Config config) {
        this.config = config;
        this.config.registerForUpdates(config1 -> {
            tracing = config1.getBoolean("tracing.router");
        });
    }

    void filterRequest(int bucketId, String shardId) throws InterruptedException {
        if (globalLock != null) {
            if (tracing) {
                logger.info("Request blocked by global lock");
            }
            globalLock.await();
        }

        CountDownLatch shardLock = shards.get(shardId);
        if (shardLock != null) {
            if (tracing) {
                logger.info("Request blocked by shard lock - shardId={}", shardId);
            }
            shardLock.await();
        }

        List<CountDownLatch> bucketLocks = getBucketLocks(bucketId);
        if (bucketLocks.size() != 0) {
            for (CountDownLatch bucketLock : bucketLocks) {
                if (tracing) {
                    logger.info("Request blocked by bucket lock - bucketId={}", bucketId);
                }
                bucketLock.await();
            }
        }
    }

    long unblockRequestOnShard(String shardId) {
        CountDownLatch lock = shards.remove(shardId);
        if (lock != null) {
            // TODO: this is not the expected blocking count.
            long count = lock.getCount();
            lock.countDown();
            if (tracing) {
                logger.info("Request unblocked on shardId={}", shardId);
            }
            return count;
        }
        return 0;
    }

    void blockRequestOnShard(String shardId) {
        if (tracing) {
            logger.info("Block requests on shard : {}", shardId);
        }
        shards.putIfAbsent(shardId, new CountDownLatch(1));
    }

    void unblockRequest() {
        if (tracing) {
            logger.info("Unblock all requests");
        }
        if (globalLock != null) {
            globalLock.countDown();
            globalLock = null;
        }
    }

    void blockRequest() {
        if (tracing) {
            logger.info("Block all requests");
        }
        globalLock = new CountDownLatch(1);
    }

    void unblockRequestOnBuckets(Range<Integer> splitRange) {
        if (tracing) {
            logger.info("Unblock requests on buckets : {}", splitRange);
        }
        CountDownLatch lock = buckets.remove(splitRange);
        if (lock != null) {
            lock.countDown();
        }
    }


    void blockRequestOnBuckets(Range<Integer> splitRange) {
        if (tracing) {
            logger.info("Block requests on buckets : {}", splitRange);
        }
        buckets.putIfAbsent(splitRange, new CountDownLatch(1));
    }

    private List<CountDownLatch> getBucketLocks(int bucketId) throws InterruptedException {
        rwLock.readLock().lock();
        try {
            return buckets.entrySet().stream()
                .filter(e -> e.getKey().contains(bucketId))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
        } finally {
            rwLock.readLock().unlock();
        }
    }
}
