/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.yahoo.gondola.Config;

import org.apache.curator.test.TestingServer;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class RegistryClientsTest {

    public static final String REGISTRY_ZOOKEEPER_SERVERS = "registry_zookeeper.servers";
    @Mock
    Config config;

    TestingServer testingServer;

    @Test
    public void testConfigFormat() {
        URL resource = RegistryClientsTest.class.getClassLoader().getResource("gondola.conf");
        if (resource == null) {
            throw new IllegalStateException("Cannot open gondola.conf");
        }
        Config config = new Config(new File(resource.getFile()));
        List<String> list = config.getList(REGISTRY_ZOOKEEPER_SERVERS);
        assertEquals(list.size(), 3);
        assertEquals(list.get(0), "zk1.yahoo.com:2181");

    }

    @Test
    public void testCreateZookeeperClient() throws Exception {
        MockitoAnnotations.initMocks(this);
        testingServer = new TestingServer();
        when(config.getList(eq(REGISTRY_ZOOKEEPER_SERVERS)))
            .thenReturn(Arrays.asList(testingServer.getConnectString()));
        RegistryClient registryClient = RegistryClients.createZookeeperClient(config);
        assertNotNull(registryClient);
        assertEquals(registryClient.getEntries().size(), 0);
    }
}
