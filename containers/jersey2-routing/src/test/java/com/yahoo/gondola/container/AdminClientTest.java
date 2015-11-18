package com.yahoo.gondola.container;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.client.ShardManagerClient;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;

public class AdminClientTest {

    public static final String SERVICE_NAME = "serviceName";
    AdminClient adminClient;

    @Mock
    Config config;

    @Mock
    ShardManagerClient shardManagerClient;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        adminClient = new AdminClient("init", shardManagerClient, config);
    }

    @Test
    public void testSetAndGetServiceName() throws Exception {
        assertNotEquals(adminClient.getServiceName(), SERVICE_NAME);
        adminClient.setServiceName(SERVICE_NAME);
        assertEquals(adminClient.getServiceName(), SERVICE_NAME);

    }

    @Test
    public void testGetAndSetConfig() throws Exception {
        assertNull(adminClient.getConfig());
        adminClient.setConfig(config);
        assertEquals(adminClient.getConfig(), config);
    }

    @Test
    public void testSplitShard_success() throws Exception {
        adminClient.splitShard("c1", "c2");
    }

    @Test
    public void testSplitShard_failed_on_allow_observer() throws Exception {
        adminClient.splitShard("c1", "c2");
    }

    @Test
    public void testSplitShard_failed_on_start_observing() throws Exception {
        adminClient.splitShard("c1", "c2");
    }

    @Test
    public void testSplitShard_failed_on_assign_bucket() throws Exception {
        adminClient.splitShard("c1", "c2");
    }

    @Test
    public void testSplitShard_failed_on_disallow_observer() throws Exception {
        adminClient.splitShard("c1", "c2");
    }

    @Test
    public void testSplitShard_failed() throws Exception {
        adminClient.splitShard("c1", "c2");
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