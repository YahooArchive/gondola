package com.yahoo.gondola.container;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.ShardManagerProtocol.ShardManagerException;
import com.yahoo.gondola.container.client.ShardManagerClient;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.yahoo.gondola.container.ShardManagerProtocol.ShardManagerException.CODE.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class AdminClientTest {

    public static final String SERVICE_NAME = "serviceName";
    AdminClient adminClient;

    @Mock
    Config config;

    @Mock
    ShardManagerClient shardManagerClient;

    @Mock
    ConfigWriter configWriter;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        adminClient = new AdminClient("init", shardManagerClient, config, configWriter);
    }

    @Test
    public void testSetAndGetServiceName() throws Exception {
        assertNotEquals(adminClient.getServiceName(), SERVICE_NAME);
        adminClient.setServiceName(SERVICE_NAME);
        assertEquals(adminClient.getServiceName(), SERVICE_NAME);

    }

    @Test
    public void testGetAndSetConfig() throws Exception {
        // TODO: implement
    }

    @Test
    public void testSplitShard_success() throws Exception {
        when(shardManagerClient.waitSlavesApproaching(any(), anyLong())).thenReturn(true);
        adminClient.splitShard("c1", "c2");
    }

    @Test
    public void testSplitShard_failed_on_start_observing() throws Exception {
        doThrow(new ShardManagerException(FAILED_START_SLAVE))
            .when(shardManagerClient).startObserving(any(), any(), anyLong());
        try {
            adminClient.splitShard("c1", "c2");
        } catch (AdminClient.AdminException e) {
            assertEquals(e.getClass(), AdminClient.AdminException.class);
        }
        verify(shardManagerClient, times(AdminClient.RETRY_COUNT)).stopObserving(any(), any(), anyLong());
    }

    @Test
    public void testSplitShard_failed_on_wait_slave_approaching() throws Exception {
        doThrow(new ShardManagerException(FAILED_START_SLAVE))
            .when(shardManagerClient).waitSlavesApproaching(any(), anyLong());
        try {
            adminClient.splitShard("c1", "c2");
        } catch (AdminClient.AdminException e) {
            assertEquals(e.getClass(), AdminClient.AdminException.class);
        }
        verify(shardManagerClient, times(AdminClient.RETRY_COUNT)).stopObserving(any(), any(), anyLong());

    }

    @Test
    public void testSplitShard_failed_on_migrate_buckets() throws Exception {
        doThrow(new ShardManagerException(FAILED_START_SLAVE))
            .when(shardManagerClient).migrateBuckets(any(), any(), any(), anyLong());
        try {
            adminClient.splitShard("c1", "c2");
        } catch (AdminClient.AdminException e) {
            assertEquals(e.getClass(), AdminClient.AdminException.class);
        }
        verify(shardManagerClient, times(AdminClient.RETRY_COUNT)).stopObserving(any(), any(), anyLong());
    }
}