/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.Member;
import com.yahoo.gondola.RoleChangeEvent;
import com.yahoo.gondola.Shard;
import com.yahoo.gondola.container.client.ProxyClient;
import com.yahoo.gondola.container.spi.RoutingHelper;

import org.apache.log4j.PropertyConfigurator;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.Whitebox;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class RoutingFilterTest {

    public static final String MY_APP_URI = "http://localhost:8080";
    RoutingFilter router;

    @Mock
    Gondola gondola;

    @Mock
    RoutingHelper routingHelper;

    @Mock
    ContainerRequestContext request;

    @Mock
    ContainerResponseContext response;

    @Mock
    ProxyClientProvider proxyClientProvider;

    @Mock
    ProxyClient proxyClient;


    @Mock
    Shard shard;

    @Mock
    Member member;

    @Mock
    Response proxiedResponse;

    @Captor
    ArgumentCaptor<Consumer<RoleChangeEvent>> consumer;

    Config config = new Config(new File(getResourceFile("gondola.conf")));

    static {
        PropertyConfigurator.configure(getResourceFile("log4j.properties"));
    }

    @Mock
    ExtendedUriInfo uriInfo;

    @Mock
    Map<Integer, AtomicInteger> bucketRequestCounters;


    @Mock
    ChangeLogProcessor changeLogProcessor;

    @Mock
    RoutingService routingService;

    Map<String, RoutingService> routingServices = new HashMap<>();

    MultivaluedHashMap<String, String> headersMap = new MultivaluedHashMap<>();

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(gondola.getConfig()).thenReturn(this.config);
        when(gondola.getShard(any())).thenReturn(shard);
        when(gondola.getShardsOnHost()).thenReturn(Arrays.asList(shard, shard));
        when(gondola.getHostId()).thenReturn("host1");
        when(routingHelper.getBucketHash(any())).thenReturn(1);
        when(proxyClientProvider.getProxyClient(any())).thenReturn(proxyClient);
        when(shard.getShardId()).thenReturn("shard1", "shard2");
        when(request.getUriInfo()).thenReturn(uriInfo);
        when(request.getHeaders()).thenReturn(headersMap);
        when(uriInfo.getPath()).thenReturn("/foo");
        when(request.getProperty(eq("bucketId"))).thenReturn(1);
        when(request.getProperty(eq("shardId"))).thenReturn("shard1");
        routingServices.put("shard1", routingService);
        routingServices.put("shard2", routingService);

        router = new RoutingFilter(gondola, routingHelper, proxyClientProvider, routingServices, changeLogProcessor);
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
        when(proxiedResponse.getHeaderString(eq(RoutingFilter.X_GONDOLA_ERROR)))
            .thenReturn("foo")
            .thenReturn(null);
        when(proxyClient.proxyRequest(any(), any()))
            .thenReturn(proxiedResponse);
        String newLeaderUri = getRoutingTable(router).get("shard1").get(1);
        router.filter(request);
        verify(request).abortWith(response.capture());
        assertEquals(getRoutingTable(router).get("shard1").get(0), newLeaderUri);
    }

    /**
     * The test will test if the request sending to another shard. (shardId = 2)
     */
    @Test
    public void testRouting_redirect_request_another_shard() throws Exception {
        reset(routingHelper);
        when(request.getProperty(eq("shardId"))).thenReturn("shard2");
        when(routingHelper.getBucketHash(any())).thenReturn(101); // shard2
        when(shard.getLeader()).thenReturn(member);
        ArgumentCaptor<Response> response = ArgumentCaptor.forClass(Response.class);
        when(member.isLocal()).thenReturn(false);
        when(proxyClient.proxyRequest(any(), any())).thenReturn(proxiedResponse);

        // request target is a follower
        when(proxiedResponse.getHeaderString(RoutingFilter.X_GONDOLA_LEADER_ADDRESS)).thenReturn("foo_remote_addr");
        router.filter(request);
        verify(request).abortWith(response.capture());
        assertEquals(headersMap.get(RoutingFilter.X_FORWARDED_BY).get(headersMap.size() - 1), MY_APP_URI);
        assertEquals(getRoutingTable(router).get("shard2").get(0), "foo_remote_addr");

    }

    @Test
    public void testBecomeLeader_block_shard() throws Exception {
        verify(gondola).registerForRoleChanges(consumer.capture());
        when(member.isLocal()).thenReturn(true);
        when(member.getMemberId()).thenReturn(81);
        RoleChangeEvent event = new RoleChangeEvent(shard, member, member, null, null);
        consumer.getValue().accept(event);
        Thread.sleep(1000);
//        verify(routingHelperClass, times(1)).beforeServing(any());
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
    public void testBucketSplit_migrate_db_block_shards() throws Exception {
    }


    @SuppressWarnings("unchecked")
    private Map<String, List<String>> getRoutingTable(RoutingFilter filter) {
        return (Map<String, List<String>>) Whitebox.getInternalState(filter, "routingTable");
    }

    @Test
    public void testWaitNoRequestsOnBuckets_success() throws Exception {
        when(bucketRequestCounters.get(anyInt())).thenReturn(new AtomicInteger(0));
        when(bucketRequestCounters.containsKey(anyInt())).thenReturn(true);
        mockCounterMap();
        assertTrue(router.waitNoRequestsOnBuckets(Range.closed(1, 1), 100));
    }

    @Test
    public void testWaitNoRequestsOnBuckets_failed() throws Exception {
        when(bucketRequestCounters.get(anyInt())).thenReturn(new AtomicInteger(1));
        when(bucketRequestCounters.containsKey(anyInt())).thenReturn(true);
        mockCounterMap();
        assertFalse(router.waitNoRequestsOnBuckets(Range.closed(1, 1), 100));
    }

    @Test
    public void testWaitNoRequestsOnBuckets_success_blocked_and_released() throws Exception {
        when(bucketRequestCounters.containsKey(anyInt())).thenReturn(true);
        when(bucketRequestCounters.get(anyInt()))
            .thenReturn(new AtomicInteger(1))
            .thenReturn(new AtomicInteger(1))
            .thenReturn(new AtomicInteger(0));
        mockCounterMap();
        assertTrue(router.waitNoRequestsOnBuckets(Range.closed(1, 1), 100));
    }


    private void mockCounterMap() {
        Whitebox.setInternalState(router, "bucketRequestCounters", bucketRequestCounters);
    }

}
