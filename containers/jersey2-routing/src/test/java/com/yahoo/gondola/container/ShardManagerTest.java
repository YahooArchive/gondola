/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.client.ShardManagerClient;
import com.yahoo.gondola.container.client.StatClient;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ShardManagerTest {

    public static final String SHARD = "c1";
    public static final String TARGET_SHARD = "c2";
    public static final int TIMEOUT_MS = 1000;
    ShardManager shardManager;

    @Mock
    RoutingFilter filter;

    @Mock
    StatClient statClient;

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
        shardManager = new ShardManager(filter, statClient, config, shardManagerClient);
    }

    @Test
    public void testAllowObserver() throws Exception {
        assertFalse(shardManager.getAllowedObservers().contains(1));
        shardManager.allowObserver(SHARD, TARGET_SHARD);
        assertTrue(shardManager.getAllowedObservers().containsAll(getMemberIdsInShard(TARGET_SHARD)));
        // check idempotent
        shardManager.allowObserver(SHARD, TARGET_SHARD);
        assertTrue(shardManager.getAllowedObservers().containsAll(getMemberIdsInShard(TARGET_SHARD)));
    }

    private List<Integer> getMemberIdsInShard(String shardId) {
        List<Integer>
            memberIds =
            config.getMembersInShard(shardId).stream().map(Config.ConfigMember::getMemberId).collect(
                Collectors.toList());
        if (memberIds.size() == 0) {
            throw new IllegalStateException("Number of member Ids cannot be 0.");
        }
        return memberIds;
    }

    @Test
    public void testDisallowObserver() throws Exception {
        shardManager.allowObserver(SHARD, TARGET_SHARD);
        assertTrue(shardManager.getAllowedObservers().containsAll(getMemberIdsInShard(TARGET_SHARD)));
        shardManager.disallowObserver(SHARD, TARGET_SHARD);
        assertFalse(shardManager.getAllowedObservers().containsAll(getMemberIdsInShard(TARGET_SHARD)));
        // check idempotent
        shardManager.disallowObserver(SHARD, TARGET_SHARD);
        assertFalse(shardManager.getAllowedObservers().containsAll(getMemberIdsInShard(TARGET_SHARD)));
    }

    @Test
    public void testStartObserving() throws Exception {
        assertFalse(shardManager.getObservedShards().contains(TARGET_SHARD));
        shardManager.startObserving(SHARD, TARGET_SHARD);
        assertTrue(shardManager.getObservedShards().contains(TARGET_SHARD));
        shardManager.startObserving(SHARD, TARGET_SHARD);
        assertTrue(shardManager.getObservedShards().contains(TARGET_SHARD));
    }

    @Test
    public void testStopObserving() throws Exception {
        shardManager.startObserving(SHARD, TARGET_SHARD);
        assertTrue(shardManager.getObservedShards().contains(TARGET_SHARD));
        shardManager.stopObserving(SHARD, TARGET_SHARD);
        assertFalse(shardManager.getObservedShards().contains(TARGET_SHARD));
        shardManager.stopObserving(SHARD, TARGET_SHARD);
        assertFalse(shardManager.getObservedShards().contains(TARGET_SHARD));
    }

    @Test
    public void testAssignBucket() throws Exception {
        Range<Integer> r = Range.closed(1, 2);
        shardManager.assignBucket(SHARD, r, TARGET_SHARD, TIMEOUT_MS);
    }
}