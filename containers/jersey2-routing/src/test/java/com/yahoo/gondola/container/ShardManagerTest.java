/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.client.ShardManagerClient;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ShardManagerTest {

    public static final String TARGET_SHARD = "c2";
    public static final Integer FROM_MEMBER = 1;
    public static final int TIMEOUT_MS = 1000;
    ShardManager shardManager;

    @Mock
    RoutingFilter filter;

    @Mock
    LockManager lockManager;

    @Mock
    Config config;

    @Mock
    Config.ConfigMember configMember;

    @Mock
    ShardManagerClient shardManagerClient;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(filter.getLockManager()).thenReturn(lockManager);
        when(config.getMembersInShard(any())).thenReturn(Arrays.asList(configMember, configMember, configMember));
        when(configMember.getMemberId()).thenReturn(1,2,3);
        shardManager = new ShardManager(filter, config, shardManagerClient);
    }

    @Test
    public void testStartObserving() throws Exception {
        assertFalse(shardManager.getObservedShards().contains(TARGET_SHARD));
        shardManager.startObserving(FROM_MEMBER, TARGET_SHARD);
        assertTrue(shardManager.getObservedShards().contains(TARGET_SHARD));
        shardManager.startObserving(FROM_MEMBER, TARGET_SHARD);
        assertTrue(shardManager.getObservedShards().contains(TARGET_SHARD));
    }

    @Test
    public void testStopObserving() throws Exception {
        shardManager.startObserving(FROM_MEMBER, TARGET_SHARD);
        assertTrue(shardManager.getObservedShards().contains(TARGET_SHARD));
        shardManager.stopObserving(FROM_MEMBER, TARGET_SHARD);
        assertFalse(shardManager.getObservedShards().contains(TARGET_SHARD));
        shardManager.stopObserving(FROM_MEMBER, TARGET_SHARD);
        assertFalse(shardManager.getObservedShards().contains(TARGET_SHARD));
    }

    @Test
    public void testAssignBucket() throws Exception {
        Range<Integer> r = Range.closed(1, 2);
        shardManager.assignBucket(FROM_MEMBER, r, TARGET_SHARD, TIMEOUT_MS);
    }
}