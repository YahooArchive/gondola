/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.container.impl.DirectShardManagerClient;
import com.yahoo.gondola.container.spi.RoutingHelper;

import org.mockito.Mock;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class AdminClientIT {

    URL configFileURL = AdminClientIT.class.getClassLoader().getResource("gondola.conf");
    Config config = new Config(new File(configFileURL.getFile()));


    @Mock
    RoutingHelper routingHelper;

    @Mock
    CommandListenerProvider commandListenerProvider;

    @Mock
    CommandListener commandListener;

    Map<String, String> routingTable = new ConcurrentHashMap<>();
    Map<String, String> addressTable = new HashMap<>();
    Map<String, CountDownLatch> latches = new HashMap<>();
    DirectShardManagerClient shardManagerClient;
    List<Gondola> gondolas = new ArrayList<>();
    AdminClient adminClient;

    @BeforeMethod
    public void setUp() throws Exception {
        shardManagerClient = new DirectShardManagerClient(config);
        adminClient = new AdminClient("fooService", shardManagerClient, config);
        MockitoAnnotations.initMocks(this);
        when(routingHelper.getBucketId(any())).thenReturn(1);
        when(commandListenerProvider.getCommandListner(any())).thenReturn(commandListener);
        // shardId -> memberId
        // hostId -> baseUri

        for (String shardId : config.getShardIds()) {
            latches.put(shardId, new CountDownLatch(1));
        }

        for (String hostId : config.getHostIds()) {
            Gondola gondola = new Gondola(config, hostId);
            gondola.registerForRoleChanges(roleChangeEvent -> {
                if (roleChangeEvent.leader != null) {
                    latches.get(roleChangeEvent.shard.getShardId()).countDown();
                    routingTable.put(roleChangeEvent.shard.getShardId(), routingTable
                        .put(roleChangeEvent.shard.getShardId(),
                             config.getMember(roleChangeEvent.leader.getMemberId()).getHostId()));
                }
            });
            gondola.start();
            gondolas.add(gondola);
            LocalTestRoutingServer
                testServer =
                new LocalTestRoutingServer(gondola, routingHelper, new ProxyClientProvider(), commandListenerProvider);
            addressTable.put(hostId, testServer.getHostUri());

            // inject shardManager instance
            for (Config.ConfigMember m : config.getMembersInHost(hostId)) {
                shardManagerClient.getShardManagers()
                    .put(m.getMemberId(), new ShardManager(testServer.getRoutingFilter(), config, shardManagerClient));
            }
        }
        Set<Map.Entry<String, CountDownLatch>> entries = latches.entrySet();
        for (Map.Entry<String, CountDownLatch> e : entries) {
            e.getValue().await();
        }
        System.out.println("leader election completed");
    }

    @AfterMethod
    public void tearDown() throws Exception {
        gondolas.parallelStream().forEach(Gondola::stop);
    }

    @Test
    public void testAssignBuckets() throws Exception {
        adminClient.assignBuckets("shard1", "shard2", Range.closed(0, 10));
    }
}