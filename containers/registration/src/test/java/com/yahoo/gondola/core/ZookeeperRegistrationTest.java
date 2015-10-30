/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.core;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ZookeeperRegistrationTest {

    TestingServer testingServer;
    CuratorFramework client;
    ObjectMapper objectMapper = new ObjectMapper();
    Registration registration;

    @Mock
    Registration.RegistrationObserver observer;


    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        testingServer = new TestingServer();
        client = getCuratorFramework();
        registration = new ZookeeperRegistration(client, objectMapper);
    }


    @Test(dataProvider = "getInputData")
    public void testRegister(String testSiteId, String testClusterId, int expectedMemberId, Class expectedException)
        throws Exception {
        try {
            int memberId = registration.register(testClusterId, InetAddress.getLoopbackAddress(), testSiteId, URI
                .create("https://api1.yahoo.com:4443"));
            List<String> nodes = client.getChildren().forPath(ZookeeperRegistration.GONDOLA_DISCOVER);
            assertTrue(nodes.contains(testSiteId + "_" + memberId));
            Registration.Registry
                registry =
                objectMapper.readValue(client.getData().forPath(
                                           ZookeeperRegistration.GONDOLA_DISCOVER + "/" + testSiteId + "_" + memberId),
                                       Registration.Registry.class);
            assertEquals(memberId, expectedMemberId);
        } catch (Registration.RegistrationException e) {
            assertTrue(e.getClass().equals(expectedException));
        }
    }

    @DataProvider
    public Object[][] getInputData() {
        return new Object[][]{
            {"gq1", "cluster1", 7, null},
            {"bf1", "cluster1", 4, null},
            {"ne1", "cluster2", 10, null},
            {"wrong", "cluster1", 7, Registration.RegistrationException.class},
            {"ne1", "clusterWrong", 7, Registration.RegistrationException.class},
            {"wrong", "clusterWrong", 7, Registration.RegistrationException.class},
        };

    }

    @Test
    public void testAddObserver() throws Exception {
        ArgumentCaptor<Registration.Registry>
            args =
            ArgumentCaptor.forClass(Registration.Registry.class);
        registration.addObserver(observer);
        int registeredMemberId = registration.register("cluster1", InetAddress.getLoopbackAddress(), "gq1", URI.create("https://api1.yahoo.com:4443"));
        // wait for zookeeper events
        Thread.sleep(100);
        verify(observer, times(1)).registrationUpdate(args.capture());
        Registration.Registry registry = args.getValue();
        assertEquals(registry.clusterId, "cluster1");
        assertEquals(registry.memberId, registeredMemberId);
    }


    @Test
    public void testGetRegistries_local() throws Exception {
        testGetRegistries(registration, registration, 0);
    }

    @Test
    public void testGetRegistries_remote() throws Exception {
        ZookeeperRegistration reader = new ZookeeperRegistration(client, objectMapper);
        testGetRegistries(registration, reader, 100);

    }

    @Test
    public void testAwait() throws Exception {

    }

    @Test
    public void testGetConfig() throws Exception {

    }

    private void testGetRegistries(Registration writer, Registration reader, int sleep) throws Exception {
        int id1 = writer.register("cluster1", InetAddress.getLoopbackAddress(), "gq1", URI.create("https://api1.yahoo.com:4443"));
        int id2 = writer.register("cluster1", InetAddress.getLoopbackAddress(), "bf1", URI.create("https://api2.yahoo.com:4443"));
        int id3 = writer.register("cluster1", InetAddress.getLoopbackAddress(), "ne1", URI.create("https://api3.yahoo.com:4443"));
        if (sleep != 0) {
            Thread.sleep(sleep);
        }
        Map<Integer, Registration.Registry> registries = reader.getRegistries();
        // test all the entries
        assertEquals(registries.get(id1).siteId, "gq1");
        assertEquals(registries.get(id2).siteId, "bf1");
        assertEquals(registries.get(id3).siteId, "ne1");

        // test other fields
        assertEquals(registries.get(id1).clusterId, "cluster1");
        assertEquals(registries.get(id1).hostId, "assigned");
        assertEquals(registries.get(id1).memberId, id1);
        assertEquals(registries.get(id1).serverAddress, InetAddress.getLoopbackAddress());
    }


    private CuratorFramework getCuratorFramework() {
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(testingServer.getConnectString())
            .retryPolicy(new RetryOneTime(1000))
            .build();
        client.start();
        return client;
    }
}