/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.container.client.StatClient;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ShardManagerTest {

    ShardManager shardManager;

    @Mock
    RoutingFilter filter;

    @Mock
    StatClient statClient;

    @Mock
    LockManager lockManager;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(filter.getLockManager()).thenReturn(lockManager);
        shardManager = new ShardManager(filter, statClient);
    }

    @Test
    public void testAllowObserver() throws Exception {
        assertFalse(shardManager.getAllowedObservers().contains(1));
        shardManager.allowObserver(1);
        assertTrue(shardManager.getAllowedObservers().contains(1));
        // check idempotent
        shardManager.allowObserver(1);
        assertTrue(shardManager.getAllowedObservers().contains(1));
    }

    @Test
    public void testDisallowObserver() throws Exception {
        shardManager.allowObserver(1);
        assertTrue(shardManager.getAllowedObservers().contains(1));
        shardManager.disallowObserver(1);
        assertFalse(shardManager.getAllowedObservers().contains(1));
        // check idempotent
        shardManager.disallowObserver(1);
        assertFalse(shardManager.getAllowedObservers().contains(1));
    }

    @Test
    public void testStartObserving() throws Exception {
        assertFalse(shardManager.getObservedShards().contains("c1"));
        shardManager.startObserving("c1");
        assertTrue(shardManager.getObservedShards().contains("c1"));
        shardManager.startObserving("c1");
        assertTrue(shardManager.getObservedShards().contains("c1"));
    }

    @Test
    public void testStopObserving() throws Exception {
        shardManager.startObserving("c1");
        assertTrue(shardManager.getObservedShards().contains("c1"));
        shardManager.stopObserving("c1");
        assertFalse(shardManager.getObservedShards().contains("c1"));
        shardManager.stopObserving("c1");
        assertFalse(shardManager.getObservedShards().contains("c1"));
    }

    @Test
    public void testAssignBucket() throws Exception {
        Range<Integer> r = Range.closed(1, 2);
        shardManager.assignBucket(r, "c1", 1000);
    }
}