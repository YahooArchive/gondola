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
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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

    @Mock
    ChangeLogProcessor changeLogProcessor;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(filter.isLeaderInShard(any())).thenReturn(true);
        when(filter.getChangeLogProcessor()).thenReturn(changeLogProcessor);
        when(gondola.getShard(any())).thenReturn(shard);
        when(shard.getLocalMember()).thenReturn(member);
        shardManager = new ShardManager(gondola, filter, config, shardManagerClient);
    }

    @Test
    public void testStartObserving_success() throws Exception {
        Member.SlaveStatus status = getSuccessSlaveStatus();
        when(member.getSlaveStatus()).thenReturn(status);
        assertFalse(getObservedShards().contains(TARGET_SHARD));
        shardManager.startObserving(FROM_SHARD, TARGET_SHARD, 300);
        assertTrue(getObservedShards().contains(TARGET_SHARD));
        shardManager.startObserving(FROM_SHARD, TARGET_SHARD, 300);
        assertTrue(getObservedShards().contains(TARGET_SHARD));
    }

    private Member.SlaveStatus getSuccessSlaveStatus() {
        Member.SlaveStatus status = new Member.SlaveStatus();
        status.running = true;
        status.commitIndex = 100;
        status.masterId = 86;
        return status;
    }

    @Test(expectedExceptions = ShardManagerProtocol.ShardManagerException.class)
    public void testStartObserving_failed() throws Exception {
        Member.SlaveStatus status = new Member.SlaveStatus();
        status.running = false;
        when(member.getSlaveStatus()).thenReturn(status);
        assertFalse(getObservedShards().contains(TARGET_SHARD));
        shardManager.startObserving(FROM_SHARD, TARGET_SHARD, 300);
    }


    @Test
    public void testStopObserving() throws Exception {
        Member.SlaveStatus status = getSuccessSlaveStatus();
        when(member.getSlaveStatus()).thenReturn(status);
        // Start observing
        shardManager.startObserving(FROM_SHARD, TARGET_SHARD, 300);
        assertTrue(getObservedShards().contains(TARGET_SHARD));

        // Stop observing successfully
        when(member.getSlaveStatus()).thenReturn(status).thenReturn(null);
        shardManager.stopObserving(FROM_SHARD, TARGET_SHARD, 300);
        assertFalse(getObservedShards().contains(TARGET_SHARD));

        // Stop observing again
        when(member.getSlaveStatus()).thenReturn(null);
        shardManager.stopObserving(FROM_SHARD, TARGET_SHARD, 300);
        assertFalse(getObservedShards().contains(TARGET_SHARD));
    }

    @Test
    public void testAssignBucket_success() throws Exception {
        Range<Integer> r = Range.closed(1, 2);
        shardManager.migrateBuckets(r, FROM_SHARD, TARGET_SHARD, TIMEOUT_MS);
        verify(filter, times(1)).updateBucketRange(any(), any(), any(), anyBoolean());
        verify(shardManagerClient, times(1)).waitSlavesSynced(any(), anyLong());
        verify(shardManagerClient, times(1)).stopObserving(any(), any(), anyLong());
    }

    @Test
    public void testAssignBucket_failed() throws Exception {
        Range<Integer> r = Range.closed(1, 2);
        doThrow(ExecutionException.class).when(filter).waitNoRequestsOnBuckets(any(), anyLong());
        shardManager.migrateBuckets(r, FROM_SHARD, TARGET_SHARD, TIMEOUT_MS);
        verify(shardManagerClient, times(1)).startObserving(any(), any(), anyLong());
        verify(filter, times(0)).updateBucketRange(any(), any(), any(), anyBoolean());
        verify(shardManagerClient, times(0)).setBuckets(any(), any(), any(), anyBoolean());
    }

    @SuppressWarnings("unchecked")
    private Set<String> getObservedShards() {
        return (Set<String>) Whitebox.getInternalState(shardManager, "observedShards");
    }

    @Test
    public void testWaitSlavesSynced() throws Exception {
        when(shard.getCommitIndex()).thenReturn(2);
        when(shard.getLastSavedIndex()).thenReturn(1).thenReturn(2);
        when(shard.getMember(anyInt())).thenReturn(member);
        when(member.getSlaveStatus()).thenReturn(getSuccessSlaveStatus());
        when(changeLogProcessor.getAppliedIndex(any())).thenReturn(100);
        assertTrue(shardManager.waitSlavesSynced(FROM_SHARD, 300));
    }

    @Test
    public void testWaitSlavesSynced_failed_never_catched_up() throws Exception {
        when(shard.getCommitIndex()).thenReturn(1+1);
        when(shard.getLastSavedIndex()).thenReturn(1);
        when(shard.getMember(anyInt())).thenReturn(member);
        when(member.getSlaveStatus()).thenReturn(getSuccessSlaveStatus());
        assertFalse(shardManager.waitSlavesSynced(FROM_SHARD, 300));
    }

    @Test
    public void testWaitSlavesApproaching() throws Exception {
        when(shard.getCommitIndex()).thenReturn(2);
        when(shard.getLastSavedIndex()).thenReturn(1).thenReturn(2);
        when(shard.getMember(anyInt())).thenReturn(member);
        when(member.getSlaveStatus()).thenReturn(getSuccessSlaveStatus());
        assertTrue(shardManager.waitSlavesApproaching(FROM_SHARD, 300));
    }


    @Test
    public void testWaitSlavesSynced_failed_never_approaching() throws Exception {
        when(shard.getCommitIndex()).thenReturn(1 + ShardManager.LOG_APPROACHING_DIFF + 1);
        when(shard.getLastSavedIndex()).thenReturn(1);
        when(shard.getMember(anyInt())).thenReturn(member);
        when(member.getSlaveStatus()).thenReturn(getSuccessSlaveStatus());
        assertFalse(shardManager.waitSlavesSynced(FROM_SHARD, 300));
    }

}
