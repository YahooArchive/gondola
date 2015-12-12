/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.yahoo.gondola.Config;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;
import java.util.Map;

public class GondolaAdminClientTest {

    URL configUrl = ShardManagerTest.class.getClassLoader().getResource("gondola.conf");
    Config config = new Config(new File(configUrl.getFile()));
    GondolaAdminClient gondolaAdminClient;

    @BeforeMethod
    public void setUp() throws Exception {
        gondolaAdminClient = new GondolaAdminClient(config);
    }

    @Test
    public void testSetLeader() throws Exception {
        Map map = gondolaAdminClient.setLeader("host1", "shard1");
    }

    @Test
    public void testGetHostStatus() throws Exception {
        Map hostStatus = gondolaAdminClient.getHostStatus("host1");
    }

    @Test
    public void testGetServiceStatus() throws Exception {
        Map serviceStatus = gondolaAdminClient.getServiceStatus();
    }
}