/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.Shard;
import com.yahoo.gondola.container.client.ProxyClient;
import com.yahoo.gondola.container.spi.RoutingHelper;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;

import javax.ws.rs.core.MultivaluedMap;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

/**
 * Test for test harness..
 */
public class LocalTestRoutingServerTest {

    private static final String MY_APP_URI = "http://localhost:4080/";
    LocalTestRoutingServer server;

    @Mock
    Gondola gondola;

    @Mock
    RoutingHelper routingHelper;

    @Mock
    ProxyClientProvider proxyClientProvider;

    @Mock
    ShardManagerProvider shardManagerProvider;

    @Mock
    ShardManagerServer shardManagerServer;

    @Mock
    Shard shard;

    @Mock
    ProxyClient proxyClient;

    @Mock
    ContainerRequest request;

    @Mock
    ExtendedUriInfo uriInfo;

    @Mock
    MultivaluedMap<String, String> headersMap;

    @Mock
    RoutingService routingService;

    @Mock
    ChangeLogProcessor changeLogProcessor;

    URL configUri = LocalTestRoutingServerTest.class.getClassLoader().getResource("gondola.conf");
    Config config = new Config(new File(configUri.getFile()));

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(shardManagerProvider.getShardManagerServer(any(), any())).thenReturn(shardManagerServer);
        when(gondola.getConfig()).thenReturn(config);
        when(gondola.getShard(any())).thenReturn(shard);
        when(gondola.getShardsOnHost()).thenReturn(Arrays.asList(shard, shard));
        when(gondola.getHostId()).thenReturn("host1");
        when(routingHelper.getBucketHash(any())).thenReturn(1);
        when(proxyClientProvider.getProxyClient(any())).thenReturn(proxyClient);
        when(shard.getShardId()).thenReturn("shard1", "shard2");
        when(shardManagerProvider.getShardManagerServer(any(), any())).thenReturn(shardManagerServer);
        when(request.getUriInfo()).thenReturn(uriInfo);
        when(request.getHeaders()).thenReturn(headersMap);
        when(request.getRequestUri()).thenReturn(URI.create(MY_APP_URI));
    }

    @Test
    public void testRoutingServer() throws Exception {
        server =
            new LocalTestRoutingServer(gondola, routingHelper, proxyClientProvider,
                                       new HashMap<String, RoutingService>() {
                                           {
                                               put("shard1", routingService);
                                               put("shard2", routingService);
                                           }
                                       }, changeLogProcessor);

        CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = client.execute(new HttpGet(server.getHostUri()));
        assertEquals(response.getStatusLine().getStatusCode(), 503);
        assertEquals(EntityUtils.toString(response.getEntity()), "No leader is available");
    }
}