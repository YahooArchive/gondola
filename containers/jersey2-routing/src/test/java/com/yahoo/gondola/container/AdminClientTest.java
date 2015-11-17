package com.yahoo.gondola.container;

import com.yahoo.gondola.container.client.ShardManagerClient;
import com.yahoo.gondola.container.client.StatClient;

import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AdminClientTest {

    AdminClient adminClient;

    @Mock
    ShardManagerClient shardManagerClient;

    @Mock
    StatClient statClient;

    @BeforeMethod
    public void setUp() throws Exception {
        adminClient = new AdminClient("serviceName", shardManagerClient, statClient);

    }

    @Test
    public void testSetServiceName() throws Exception {

    }

    @Test
    public void testGetServiceName() throws Exception {

    }

    @Test
    public void testGetConfig() throws Exception {

    }

    @Test
    public void testSetConfig() throws Exception {

    }

    @Test
    public void testSplitShard() throws Exception {

    }

    @Test
    public void testMergeShard() throws Exception {

    }

    @Test
    public void testEnable() throws Exception {

    }

    @Test
    public void testDisable() throws Exception {

    }

    @Test
    public void testGetStats() throws Exception {

    }

    @Test
    public void testEnableTracing() throws Exception {

    }

    @Test
    public void testDisableTracing() throws Exception {

    }
}