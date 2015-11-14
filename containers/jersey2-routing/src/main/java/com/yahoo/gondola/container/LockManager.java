/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;


import com.google.common.collect.Range;

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
    CountDownLatch globalLock;
    Map<String, CountDownLatch> clusters = new ConcurrentHashMap<>();
    Map<Range<Integer>, CountDownLatch> buckets = new HashMap<>();
    ReadWriteLock rwLock = new ReentrantReadWriteLock();
    static Logger logger = LoggerFactory.getLogger(LockManager.class);
    boolean tracing;

    void filterRequest(int bucketId, String clusterId) throws InterruptedException {
        if (globalLock != null) {
            if (tracing) {
                logger.info("Request blocked by global lock");
            }
            globalLock.await();
        }

        CountDownLatch clusterLock = clusters.get(clusterId);
        if (clusterLock != null) {
            if (tracing) {
                logger.info("Request blocked by cluster lock - clusterId={}", clusterId);
            }
            clusterLock.await();
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

    void unblockRequestOnCluster(String clusterId) {
        logger.info("Unblock requests on cluster : {}", clusterId);
        CountDownLatch lock = clusters.remove(clusterId);
        if (lock != null) {
            lock.countDown();
        }
    }

    void blockRequestOnCluster(String clusterId) {
        logger.info("Block requests on cluster : {}", clusterId);
        clusters.putIfAbsent(clusterId, new CountDownLatch(1));
    }

    void unblockRequest() {
        logger.info("Unblock all requests");
        if (globalLock != null) {
            globalLock.countDown();
            globalLock = null;
        }
    }

    void blockRequest() {
        logger.info("Block all requests");
        globalLock = new CountDownLatch(1);
    }

    void unblockRequestOnBuckets(Range<Integer> splitRange) {
        logger.info("Unblock requests on buckets : {}", splitRange);
        CountDownLatch lock = buckets.remove(splitRange);
        if (lock != null) {
            lock.countDown();
        }
    }


    void blockRequestOnBuckets(Range<Integer> splitRange) {
        logger.info("Block requests on buckets : {}", splitRange);
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
