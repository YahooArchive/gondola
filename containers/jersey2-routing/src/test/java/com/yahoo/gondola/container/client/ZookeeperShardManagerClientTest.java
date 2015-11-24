/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.client;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.container.ShardManager;
import com.yahoo.gondola.container.impl.ZookeeperShardManagerServer;

import org.apache.curator.test.TestingServer;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

public class ZookeeperShardManagerClientTest {

    TestingServer testingServer = null;
    ZookeeperShardManagerClient client;
    Map<String, ZookeeperShardManagerServer> servers;
    Map<String, ShardManager> shardManagers;

    URL configUrl = ZookeeperShardManagerClientTest.class.getClassLoader().getResource("gondola.conf");
    Config config = new Config(new File(configUrl.getFile()));
    List<Gondola> gondolas;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        testingServer = new TestingServer();
        testingServer.start();
        servers = new HashMap<>();
        shardManagers = new HashMap<>();
        gondolas = new ArrayList<>();
        for (String hostId : config.getHostIds()) {
            Gondola gondola = new Gondola(config, hostId);
            gondola.start();
            ZookeeperShardManagerServer
                server =
                new ZookeeperShardManagerServer("foo", testingServer.getConnectString(), gondola);
            ShardManager shardManager = mock(ShardManager.class);
            server.setShardManager(shardManager);
            shardManagers.put(hostId, shardManager);
            servers.put(hostId, server);
            gondolas.add(gondola);
        }

        client = new ZookeeperShardManagerClient("foo", testingServer.getConnectString(), config);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        servers.forEach((s, server) -> server.stop());
        testingServer.stop();
        gondolas.parallelStream().forEach(Gondola::stop);
    }

    @Test
    public void testStartObserving() throws Exception {
        client.startObserving("shard2", "shard1", 1000);
        Thread.sleep(100);

        for (Config.ConfigMember m : config.getMembersInShard("shard2")) {
            ShardManager shardManager = shardManagers.get(m.getHostId());
            verify(shardManager).startObserving(any(), any(), anyLong());
        }

        for (Config.ConfigMember m : config.getMembers()) {
            if (!m.getShardId().equals("shard2")) {
                ShardManager shardManager = shardManagers.get(m.getHostId());
                verify(shardManager, times(0)).startObserving(any(), any(), anyLong());
            }
        }
    }

    @Test
    public void testStopObserving() throws Exception {
        client.stopObserving("shard2", "shard1", 1000);
        Thread.sleep(100);

        for (Config.ConfigMember m : config.getMembersInShard("shard2")) {
            ShardManager shardManager = shardManagers.get(m.getHostId());
            verify(shardManager).stopObserving(any(), any(), anyLong());
        }
    }

    @Test
    public void testMigrateBuckets() throws Exception {
        client.migrateBuckets(Range.closed(0, 10), "shard1", "shard2", 1000);
        Thread.sleep(100);
        for (Config.ConfigMember m : config.getMembersInShard("shard1")) {
            ShardManager shardManager = shardManagers.get(m.getHostId());
            verify(shardManager).migrateBuckets(any(), any(), any(), anyLong());
        }
    }

    @Test
    public void testWaitSlavesSynced() throws Exception {
        for (Config.ConfigMember m : config.getMembersInShard("shard2")) {
            ShardManager shardManager = shardManagers.get(m.getHostId());
            when(shardManager.waitSlavesSynced(any(), anyLong())).thenReturn(true);
        }
        client.startObserving("shard2", "shard1", 1000);
        assertTrue(client.waitSlavesSynced("shard2", 1000));
    }

    @Test
    public void testWaitSlavesApproaching() throws Exception {
        for (Config.ConfigMember m : config.getMembersInShard("shard2")) {
            ShardManager shardManager = shardManagers.get(m.getHostId());
            when(shardManager.waitSlavesApproaching(any(), anyLong())).thenReturn(true);
        }
        client.startObserving("shard2", "shard1", 1000);
        assertTrue(client.waitSlavesApproaching("shard2", 1000));
    }

    @Test
    public void testWaitSlavesApproaching_successs_on_synched() throws Exception {
        for (Config.ConfigMember m : config.getMembersInShard("shard2")) {
            ShardManager shardManager = shardManagers.get(m.getHostId());
            when(shardManager.waitSlavesSynced(any(), anyLong())).thenReturn(true);
        }
        client.startObserving("shard2", "shard1", 1000);
        assertTrue(client.waitSlavesApproaching("shard2", 1000));
    }


    @Test
    public void testSetBuckets() throws Exception {
        client.setBuckets(Range.closed(0, 10), "shard1", "shard2", false);
        Thread.sleep(100);
        for (String hostId : config.getHostIds()) {
            verify(shardManagers.get(hostId)).setBuckets(any(), any(), any(), anyBoolean());
        }
    }

    @Test
    public void testWaitBucketsCondition() throws Exception {
        client.setBuckets(Range.closed(0, 10), "shard1", "shard2", false);
        assertTrue(client.waitBucketsCondition(Range.closed(0, 10), "shard1", "shard2", 1000));
    }
}