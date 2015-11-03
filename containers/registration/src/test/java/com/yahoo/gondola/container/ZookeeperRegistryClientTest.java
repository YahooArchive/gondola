/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.container;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.gondola.Config;

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

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class ZookeeperRegistryClientTest {

    TestingServer testingServer;
    CuratorFramework client;
    ObjectMapper objectMapper = new ObjectMapper();
    RegistryClient registryClient;

    @Mock
    Config config;

    @Mock
    Consumer<RegistryClient.Entry> listener;

    static final String SITE_1_HOST_3_CLUSTERS = "site_1_host_3_clusters";
    static final String SITE_1_HOST_2_CLUSTERS = "site_1_host_2_clusters";
    static final String SITE_1_HOST_1_CLUSTER = "site_1_host_1_cluster";
    static final String SITE_2_HOSTS = "site_2_hosts";


    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        testingServer = new TestingServer();
        readConfig();

        client = getCuratorFramework();
        registryClient = new ZookeeperRegistryClient(client, objectMapper, config);
        registryClient.addListener(listener);
    }

    private void readConfig() {
        URL resource = ZookeeperRegistryClientTest.class.getClassLoader().getResource("gondola.conf");
        if (resource == null) {
            throw new IllegalStateException("cannot find gondola config");
        }
        config = new Config(new File(resource.getFile()));
    }

    @Test(dataProvider = "getInputData")
    public void testRegister(String testSiteId, String uri, Class expectedException)
        throws Exception {
        try {
            InetSocketAddress gondolaAddress = new InetSocketAddress(1234);
            String hostId = registryClient.register(testSiteId, gondolaAddress, URI.create(uri));
            RegistryClient.Entry
                entry =
                objectMapper.readValue(client.getData().forPath(ZookeeperRegistryClient.GONDOLA_HOSTS + "/" + hostId),
                                       RegistryClient.Entry.class);

            assertNotNull(entry);
            assertEquals(hostId, entry.hostId);
            assertEquals(testSiteId, entry.siteId);
            assertEquals(config.getClusterIds(hostId), entry.clusterIds);
            assertEquals(getMemberIdsByHostId(hostId), entry.memberIds);
            assertEquals(gondolaAddress, entry.serverAddress);
        } catch (RegistryClient.RegistryException e) {
            assertTrue(e.getClass().equals(expectedException));
        }
    }

    @Test
    public void testRegister_Insufficient() {
        registryClient.register(SITE_1_HOST_3_CLUSTERS, new InetSocketAddress(1234), URI.create("http://foo.com/"));
        try {
            registryClient.register(SITE_1_HOST_3_CLUSTERS, new InetSocketAddress(1235), URI.create("http://foo.com/"));
        } catch (RegistryClient.RegistryException e) {
            assertTrue(e.getMessage().matches(".*Unable to register hostId, all hosts are full on site.*"));
        }
    }

    @Test
    public void testRegister_2_hosts_in_1_site() {
        registryClient.register(SITE_2_HOSTS, new InetSocketAddress(1234), URI.create("http://foo.com/"));
        registryClient.register(SITE_2_HOSTS, new InetSocketAddress(1235), URI.create("http://foo.com/"));
        try {
            registryClient.register(SITE_2_HOSTS, new InetSocketAddress(1236), URI.create("http://foo.com/"));
        } catch (RegistryClient.RegistryException e) {
            assertTrue(e.getMessage().matches(".*Unable to register hostId, all hosts are full on site.*"));
        }
    }

    @Test
    public void testRegister_twice() {
        registryClient.register(SITE_1_HOST_1_CLUSTER, new InetSocketAddress(1234), URI.create("http://foo.com/"));
        registryClient.register(SITE_1_HOST_1_CLUSTER, new InetSocketAddress(1234), URI.create("http://foo.com/"));

    }

    @DataProvider
    public Object[][] getInputData() {
        return new Object[][]{
            {SITE_1_HOST_3_CLUSTERS, "http://api.yahoo.com:4080", null},
            {SITE_1_HOST_2_CLUSTERS, "http://api.yahoo.com:4080", null},
            {SITE_1_HOST_1_CLUSTER, "http://api.yahoo.com:4080", null},
            {"foo", "http://api.yahoo.com:4080", RegistryClient.RegistryException.class},
            };

    }

    @Test
    public void testAddListener() throws Exception {
        ArgumentCaptor<RegistryClient.Entry>
            args =
            ArgumentCaptor.forClass(RegistryClient.Entry.class);
        String hostId = registryClient.register(SITE_1_HOST_1_CLUSTER, new InetSocketAddress(1234),
                                                URI.create("https://api1.yahoo.com:4443"));
        // wait for zookeeper events
        Thread.sleep(100);
        verify(listener, times(1)).accept(args.capture());
        RegistryClient.Entry entry = args.getValue();
        assertEquals(entry.hostId, hostId);
    }


    @Test
    public void testGetRegistries_local() throws Exception {
        testGetEntries(registryClient, registryClient, 0);
    }

    @Test
    public void testGetRegistries_remote() throws Exception {
        ZookeeperRegistryClient reader = new ZookeeperRegistryClient(client, objectMapper, config);
        testGetEntries(registryClient, reader, 100);

    }

    @Test
    public void testAwait() throws Exception {
        // 0. A three nodes cluster, two server joins
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<Boolean> result;
        registryClient.register(SITE_1_HOST_3_CLUSTERS, new InetSocketAddress(1234), URI.create("http://foo.com"));
        registryClient.register(SITE_1_HOST_2_CLUSTERS, new InetSocketAddress(1235), URI.create("http://foo.com"));

        // 1. The await call should block for 1 second
        Callable<Boolean> awaitCall = () -> registryClient.await(1000);

        result = executorService.submit(awaitCall);
        assertEquals(result.get(), Boolean.FALSE);

        // 2. The request should be rnblock after the next node joins
        result = executorService.submit(awaitCall);
        registryClient.register(SITE_1_HOST_1_CLUSTER, new InetSocketAddress(1236), URI.create("http://foo.com"));
        assertEquals(result.get(), Boolean.TRUE);

        // 3. The request should success immediately, since all nodes are in the clusters.
        result = executorService.submit(awaitCall);
        assertEquals(result.get(), Boolean.TRUE);
    }

    private void testGetEntries(RegistryClient writer, RegistryClient reader, int sleep) throws Exception {
        String
            hostId =
            writer.register(SITE_1_HOST_3_CLUSTERS, new InetSocketAddress(1234),
                            URI.create("https://api1.yahoo.com:4443"));

        List<RegistryClient.Entry> writerEntries = writer.getEntries().entrySet().stream()
            .map(Map.Entry::getValue)
            .filter(e -> e.hostId.equals(hostId))
            .collect(Collectors.toList());

        assertEquals(writerEntries.size(), 1);

        if (sleep != 0) {
            Thread.sleep(sleep);
        }

        Map<String, RegistryClient.Entry> readerEntries = reader.getEntries();
        assertEquals(readerEntries.size(), 1);

        for (RegistryClient.Entry e : writerEntries) {
            RegistryClient.Entry readerEntry = readerEntries.get(e.hostId);
            assertEquals(readerEntry.hostId, e.hostId);
            assertEquals(readerEntry.siteId, e.siteId);
            assertEquals(readerEntry.serverAddress, e.serverAddress);
            assertEquals(readerEntry.clusterIds, e.clusterIds);
            assertEquals(readerEntry.memberIds, e.memberIds);
        }
    }


    private CuratorFramework getCuratorFramework() {
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(testingServer.getConnectString())
            .retryPolicy(new RetryOneTime(1000))
            .build();
        client.start();
        return client;
    }

    private List<Integer> getMemberIdsByHostId(String hostId) {
        return config.getClusterIds(hostId).stream()
            .map(s -> config.getMembersInCluster(s))
            .flatMap(Collection::stream)
            .filter(configMember -> configMember.getHostId().equals(hostId))
            .map(Config.ConfigMember::getMemberId)
            .collect(Collectors.toList());
    }
}
