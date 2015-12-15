/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.codahale.metrics.Timer;
import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.Member;
import com.yahoo.gondola.Shard;

import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.server.ContainerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;

/**
 * JAX-RS Gondola Admin Resource.
 */

@Path("/gondola/v1/local")
@Produces("application/json")
@Singleton
public class GondolaAdminResource {
    static Logger logger = LoggerFactory.getLogger(GondolaAdminResource.class);

    @POST
    @Path("/setLeader")
    public Map setLeader(@QueryParam("shardId") String shardId) {
        Map<Object, Object> result = new HashMap<>();
        try {
            GondolaApplication.getRoutingFilter().getGondola().getShard(shardId).forceLeader(5000);
            result.put("success", true);
        } catch (Exception e) {
            result.put("success", false);
            result.put("reason", e.getMessage());
        }
        return result;
    }

    @POST
    @Path("/enable")
    public Map enable(@QueryParam("shardId") String shardId, @QueryParam("enable") boolean enable) {
        Map<Object, Object> result = new HashMap<>();
        Member localMember = GondolaApplication.getRoutingFilter().getGondola().getShard(shardId).getLocalMember();
        try {
            localMember.enable(enable);
            result.put("success", true);
        } catch (Exception e) {
            result.put("success", false);
            result.put("reason", e.getMessage());
        }
        return result;
    }

    @GET
    @Path("/gondolaStatus")
    public Map getGondolaStatus() throws InterruptedException {
        RoutingFilter routingFilter = GondolaApplication.getRoutingFilter();
        Gondola gondola = routingFilter.getGondola();
        ChangeLogProcessor changeLogProcessor = GondolaApplication.getRoutingFilter().getChangeLogProcessor();
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put("hostId", gondola.getHostId());
        map.put("gondolaStatus", getGondolaStatus(gondola, changeLogProcessor));
        map.put("routingTable", routingFilter.getRoutingTable());
        map.put("bucketTable", getBucketMapStatus(routingFilter.getBucketManager()));
        map.put("lock", getLockManagerStatus(routingFilter.getLockManager()));
        map.put("shardManager", getShardManagerStatus());
        map.put("config", getConfigInfo(gondola));
        map.put("stats", gondola.getStats());
        map.put("pid", gondola.getConfig().getAttributesForHost(gondola.getHostId()).get("hostname")
                       + ":" + gondola.getProcessId());
        map.put("timers", timerMap());
        map.put("meters", GondolaApplication.MyMetricsServletContextListener.METRIC_REGISTRY.getMeters());

        return map;
    }

    private Map timerMap() {
        Map<String, Object> map = new HashMap<>();
        SortedMap<String, Timer> timers =
            GondolaApplication.MyMetricsServletContextListener.METRIC_REGISTRY.getTimers();
        for (Map.Entry<String, Timer> e : timers.entrySet()) {
            Map<String, Object> data = new HashMap<>();
            data.put("oneMinuteRate", e.getValue().getOneMinuteRate());
            data.put("meanResponseTime", e.getValue().getSnapshot().getMean()/1000/1000);
            map.put(e.getKey(), data);
        }
        return map;
    }

    private Map getConfigInfo(Gondola gondola) {
        Map<Object, Object> map = new LinkedHashMap<>();
        Config config = gondola.getConfig();
        map.put("file", config.getFile());
        map.put("members", config.getMembers());
        map.put("shards",
                config.getShardIds().stream()
                    .map(config::getAttributesForShard)
                    .collect(Collectors.toList()));
        map.put("hosts",
                config.getHostIds().stream()
                    .map(config::getAttributesForHost)
                    .collect(Collectors.toList()));
        return map;
    }

    @GET
    @Path("/requestInspect")
    public Map getRequestInfo(@QueryParam("requestUri") String requestUri, @Context ContainerRequestContext request) {
        ContainerRequest
            containerRequest =
            new ContainerRequest(request.getUriInfo().getBaseUri(), URI.create(requestUri), "GET", null,
                                 new MapPropertiesDelegate());
        RoutingFilter routingFilter = GondolaApplication.getRoutingFilter();
        routingFilter.extractShardAndBucketIdFromRequest(containerRequest);
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put("bucketId", containerRequest.getProperty("bucketId"));
        map.put("shardId", containerRequest.getProperty("shardId"));
        return map;
    }

    private Map<Object, Object> getGondolaStatus(Gondola gondola, ChangeLogProcessor changeLogProcessor)
        throws InterruptedException {
        Map<Object, Object> shards = new LinkedHashMap<>();
        for (Shard shard : gondola.getShardsOnHost()) {
            String shardId = shard.getShardId();
            Map<Object, Object> shardMap = new LinkedHashMap<>();
            shardMap.put("commitIndex", shard.getCommitIndex());
            shardMap.put("savedIndex", shard.getLastSavedIndex());
            shardMap.put("appliedIndex", changeLogProcessor.getAppliedIndex(shardId));
            shardMap.put("slaveStatus", getSlaveStatus(shard));
            shardMap.put("role", shard.getLocalRole());
            shardMap.put("enabled", shard.getLocalMember().isEnabled());
            shards.put(shardId, shardMap);
        }
        return shards;
    }

    private Map getShardManagerStatus() {
        Map<Object, Object> map = new LinkedHashMap<>();
        ShardManagerServer shardManagerServer = GondolaApplication.getShardManagerServer();
        if (shardManagerServer != null) {
            map.put("shardManagerServer", shardManagerServer.getStatus());
        }
        return map;
    }

    private Map getLockManagerStatus(LockManager lockManager) {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put("globalLock", lockManager.getGlobalLock());
        map.put("shardLocks", lockManager.getShardLocks());
        map.put("bucketLocks", lockManager.getBucketLocks());
        return map;
    }

    private Map getBucketMapStatus(BucketManager bucketManager) {
        Map<Object, Object> map = new LinkedHashMap<>();
        for (Map.Entry<Range<Integer>, BucketManager.ShardState> e : bucketManager.getBucketMap()
            .asMapOfRanges().entrySet()) {
            map.put(e.getKey(), e.getValue());
        }
        return map;
    }

    private Member.SlaveStatus getSlaveStatus(Shard shard) {
        try {
            return shard.getLocalMember().getSlaveStatus();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
