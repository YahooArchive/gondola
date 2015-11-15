/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.yahoo.gondola.*;
import com.yahoo.gondola.Shard;
import com.yahoo.gondola.container.client.ProxyClient;
import com.yahoo.gondola.container.spi.RoutingHelper;

import org.apache.log4j.PropertyConfigurator;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.function.Consumer;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class RoutingFilterTest {

    public static final String MY_APP_URI = "http://localhost:8080";
    RoutingFilter router;

    @Mock
    Gondola gondola;

    final Logger logger = LoggerFactory.getLogger(RoutingFilterTest.class);

    @Mock
    RoutingHelper routingHelper;

    @Mock
    ContainerRequest request;

    @Mock
    ContainerResponse response;

    @Mock
    ProxyClientProvider proxyClientProvider;

    @Mock
    ProxyClient proxyClient;


    @Mock
    Shard shard;

    @Mock
    Member member;

    @Mock
    Response proxedResponse;

    @Mock
    CommandListenerProvider commandListenerProvider;

    @Mock
    CommandListener commandListener;

    @Captor
    ArgumentCaptor<Consumer<RoleChangeEvent>> consumer;

    Config config = new Config(new File(getResourceFile("gondola.conf")));

    LockManager lockManager;

    static {
        PropertyConfigurator.configure(getResourceFile("log4j.properties"));
    }

    @Mock
    ExtendedUriInfo uriInfo;

    MultivaluedHashMap<String, String> headersMap = new MultivaluedHashMap<>();

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(gondola.getConfig()).thenReturn(this.config);
        when(gondola.getShard(any())).thenReturn(shard);
        when(gondola.getShardsOnHost()).thenReturn(Arrays.asList(shard, shard));
        when(routingHelper.getBucketId(any())).thenReturn(1);
        when(proxyClientProvider.getProxyClient(any())).thenReturn(proxyClient);
        when(shard.getShardId()).thenReturn("shard1", "shard2");
        when(commandListenerProvider.getCommandListner(any())).thenReturn(commandListener);
        when(request.getUriInfo()).thenReturn(uriInfo);
        when(request.getHeaders()).thenReturn(headersMap);
        when(request.getRequestUri()).thenReturn(URI.create(MY_APP_URI));

        router = new RoutingFilter(gondola, routingHelper, proxyClientProvider, commandListenerProvider);
    }

    private static String getResourceFile(String file) {
        URL resource = RoutingFilterTest.class.getClassLoader().getResource(file);
        return resource == null ? "" : resource.getFile();
    }

    @Test
    public void testRouting_under_leader_election() throws Exception {
        when(shard.getLeader()).thenReturn(null);
        ArgumentCaptor<Response> response = ArgumentCaptor.forClass(Response.class);
        router.filter(request);
        verify(request).abortWith(response.capture());
        assertEquals(response.getValue().getEntity().toString(), "No leader is available");

    }

    @Test
    public void testRouting_accept_request_on_leader() throws Exception {
        when(shard.getLeader()).thenReturn(member);
        when(member.isLocal()).thenReturn(true);
        router.filter(request);
        verify(request, times(0)).abortWith(any());
    }

    @Test
    public void testRouting_redirect_request_on_non_leader() throws Exception {
        when(shard.getLeader()).thenReturn(member);
        ArgumentCaptor<Response> response = ArgumentCaptor.forClass(Response.class);
        when(member.isLocal()).thenReturn(false);
        when(proxyClient.proxyRequest(any(),any())).thenReturn(proxedResponse);
        router.filter(request);
        verify(request).abortWith(response.capture());
    }

    /**
     * The test will test if the request sending to another cluster. (clusterId = 2)
     */
    @Test
    public void testRouting_redirect_request_another_cluster() throws Exception {
        reset(routingHelper);
        when(routingHelper.getBucketId(any())).thenReturn(101);
        when(shard.getLeader()).thenReturn(member);
        ArgumentCaptor<Response> response = ArgumentCaptor.forClass(Response.class);
        when(member.isLocal()).thenReturn(false);
        when(proxyClient.proxyRequest(any(),any())).thenReturn(proxedResponse);


        // request traget is a follower
        when(proxedResponse.getHeaderString(RoutingFilter.X_GONDOLA_LEADER_ADDRESS)).thenReturn("foo_remote_addr");
        router.filter(request);
        verify(request).abortWith(response.capture());
        assertEquals(headersMap.get(RoutingFilter.X_FORWARDED_BY).get(headersMap.size()-1), MY_APP_URI);
        assertEquals(router.routingTable.get("cluster2").get(0), "foo_remote_addr");

    }

    @Test
    public void testBecomeLeader_block_cluster() throws Exception {
        verify(gondola).registerForRoleChanges(consumer.capture());
        when(member.isLocal()).thenReturn(true);
        when(member.getMemberId()).thenReturn(81);
        RoleChangeEvent event = new RoleChangeEvent(shard, member, member, null, null);
        consumer.getValue().accept(event);
        Thread.sleep(1000);
        verify(routingHelper, times(1)).clearState(any());
    }

    @Test
    public void testRoutingLoop() throws Exception {
        when(request.getHeaderString(any())).thenReturn("foo;" + MY_APP_URI + ";bar");
        router.filter(request);
        ArgumentCaptor<Response> response = ArgumentCaptor.forClass(Response.class);
        verify(request, times(1)).abortWith(response.capture());
        assertEquals(response.getValue().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test
    public void testBucketSplit_migrate_app_block_buckets() throws Exception {
    }

    @Test
    public void testBucketSplit_migrate_db_block_clusters() throws Exception {
    }

}
