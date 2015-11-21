/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.Member;
import com.yahoo.gondola.Shard;
import com.yahoo.gondola.container.client.ShardManagerClient;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.Whitebox;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;
import java.util.Set;
import java.util.function.Consumer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ShardManagerTest {

    public static final String FROM_SHARD = "shard1";
    public static final String TARGET_SHARD = "shard2";
    public static final int TIMEOUT_MS = 1000;
    ShardManager shardManager;

    @Mock
    RoutingFilter filter;

    @Mock
    LockManager lockManager;

    URL configUrl = ShardManagerTest.class.getClassLoader().getResource("gondola.conf");
    Config config = new Config(new File(configUrl.getFile()));

    @Mock
    Config.ConfigMember configMember;

    @Mock
    ShardManagerClient shardManagerClient;

    @Mock
    Gondola gondola;

    @Mock
    Shard shard;

    @Mock
    Member member;

    @Mock
    Consumer<Member.SlaveStatus> updateCallback;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(filter.isLeaderInShard(any())).thenReturn(true);
        when(filter.getGondola()).thenReturn(gondola);
        when(gondola.getShard(any())).thenReturn(shard);
        when(shard.getLocalMember()).thenReturn(member);
        shardManager = new ShardManager(filter, config, shardManagerClient);
    }

    @Test
    public void testStartObserving_success() throws Exception {
        callbackAnswer(86, true, null);
        assertFalse(getObservedShards().contains(TARGET_SHARD));
        shardManager.startObserving(FROM_SHARD, TARGET_SHARD);
        assertTrue(getObservedShards().contains(TARGET_SHARD));
        shardManager.startObserving(FROM_SHARD, TARGET_SHARD);
        assertTrue(getObservedShards().contains(TARGET_SHARD));
    }

    @Test(expectedExceptions = ShardManagerProtocol.ShardManagerException.class)
    public void testStartObserving_failed() throws Exception {
        callbackAnswer(86, false, null);
        assertFalse(getObservedShards().contains(TARGET_SHARD));
        shardManager.startObserving(FROM_SHARD, TARGET_SHARD);
    }


    @Test
    public void testStopObserving() throws Exception {
        // Start observing
        callbackAnswer(86, true, null);
        shardManager.startObserving(FROM_SHARD, TARGET_SHARD);
        assertTrue(getObservedShards().contains(TARGET_SHARD));

        // Stop observing successfully
        callbackAnswer(-1, true, null);
        shardManager.stopObserving(FROM_SHARD, TARGET_SHARD);
        assertFalse(getObservedShards().contains(TARGET_SHARD));

        // Stop observing again
        callbackAnswer(-1, true, null);
        shardManager.stopObserving(FROM_SHARD, TARGET_SHARD);
        assertFalse(getObservedShards().contains(TARGET_SHARD));
    }

    @Test
    public void testAssignBucket() throws Exception {
        Range<Integer> r = Range.closed(1, 2);
        shardManager.migrateBuckets(r, FROM_SHARD, TARGET_SHARD, TIMEOUT_MS);
    }

    @SuppressWarnings("unchecked")
    private void callbackAnswer(int memberId, boolean running, Exception e) {
        Member.SlaveStatus slaveStatus;
        if (memberId == -1) {
            slaveStatus = null;
        } else {
            slaveStatus = new Member.SlaveStatus();
            slaveStatus.running = running;
            slaveStatus.memberId = memberId;
            slaveStatus.exception = e;
        }
        doAnswer(invocation -> {
            ((Consumer<Member.SlaveStatus>) invocation.getArguments()[1]).accept(slaveStatus);
            return null;
        }).when(member).setSlave(anyInt(), any());
        when(member.getSlaveUpdate()).thenReturn(slaveStatus);
    }

    @SuppressWarnings("unchecked")
    private Set<String> getObservedShards() {
        return (Set<String>) Whitebox.getInternalState(shardManager, "observedShards");
    }
}
