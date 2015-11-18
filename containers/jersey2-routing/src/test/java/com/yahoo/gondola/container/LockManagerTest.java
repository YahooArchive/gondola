/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;

import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class LockManagerTest {

    LockManager lockManager;
    ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
    @Mock
    Config config;

    @BeforeMethod
    public void setUp() throws Exception {
        when(config.getBoolean(eq("tracing.router"))).thenReturn(false);
        lockManager = new LockManager(config);
    }


    @Test
    public void testUnblockRequest() throws Exception {
        // lock first
        lockManager.blockRequest();
        assertTrue(checkBlocked(1,"c1"));

        // unblock
        lockManager.unblockRequest();
        assertFalse(checkBlocked(1,"c1"));

        // unblock twice
        lockManager.unblockRequest();
        assertFalse(checkBlocked(1,"c1"));
    }



    @Test
    public void testBlockRequest() throws Exception {
        lockManager.blockRequest();
        assertTrue(checkBlocked(1,"c1"));
    }

    @Test
    public void testBlockRequestOnCluster() throws Exception {
        lockManager.blockRequestOnShard("c1");
        assertTrue(checkBlocked(1000,"c1"));
    }


    @Test
    public void testUnblockRequestOnCluster() throws Exception {
        lockManager.blockRequestOnShard("c1");
        assertTrue(checkBlocked(1000,"c1"));
        lockManager.unblockRequestOnShard("c1");
        assertFalse(checkBlocked(1000,"c1"));
        lockManager.unblockRequestOnShard("c1");
        assertFalse(checkBlocked(1000,"c1"));
    }

    @Test
    public void testBlockRequestOnBuckets() throws Exception {
        lockManager.blockRequestOnBuckets(Range.closed(1, 100));
        assertTrue(checkBlocked(1,"c1"));
        assertFalse(checkBlocked(101,"c1"));
    }

    @Test
    public void testUnblockRequestOnBuckets() throws Exception {
        lockManager.blockRequestOnBuckets(Range.closed(1, 100));
        assertTrue(checkBlocked(1,"c1"));
        lockManager.unblockRequestOnBuckets(Range.closed(1, 100));
        assertFalse(checkBlocked(1,"c1"));
        lockManager.unblockRequestOnBuckets(Range.closed(1, 100));
        assertFalse(checkBlocked(1,"c1"));
    }

    private boolean checkBlocked(int bucketId, String clusterId) throws InterruptedException {
        Future<?> result = singleThreadExecutor.submit(() -> {
            try {
                lockManager.filterRequest(bucketId, clusterId);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(50);
        if (result.isDone()) {
            return false;
        }
        result.cancel(true);
        return result.isCancelled();
    }
}