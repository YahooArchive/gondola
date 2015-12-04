/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Range;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.Member;
import com.yahoo.gondola.Shard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

/**
 * JAX-RS Admin Resource
 */

@Path("/gondola/admin/v1")
@Produces("application/json")
@Singleton
public class AdminResource {

    static {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.WRITE_NULL_MAP_VALUES);
        objectMapper.setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE);
        objectMapper.setVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.NONE);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    }

    static Logger logger = LoggerFactory.getLogger(AdminResource.class);

    @POST
    @Path("/setLeader")
    public void setLeader(@QueryParam("shardId") String shardId) {
        GondolaApplication.getRoutingFilter().getGondola().getShard(shardId).forceLeader(5000);
    }

    @GET
    @Path("/gondolaStatus")
    public Map getGondolaStatus() {
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
        map.put("config", gondola.getConfig());
        map.put("stats", gondola.getStats());
        return map;
    }

    @GET
    @Path("/requestInspect")
    public Map getRequestInfo(@QueryParam("requestUri") String requestUri) {
        // TODO: current routingHelper only accept ContainerRequestContext, see if we can make it more specific to requestURI.
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put("bucketId", 0);
        map.put("shardId", "shard1");
        return map;
    }

    private Map<Object, Object> getGondolaStatus(Gondola gondola, ChangeLogProcessor changeLogProcessor) {
        Map<Object, Object> shards = new LinkedHashMap<>();
        for (Shard shard : gondola.getShardsOnHost()) {
            String shardId = shard.getShardId();
            Map<Object, Object> shardMap = new LinkedHashMap<>();
            shardMap.put("commitIndex", shard.getCommitIndex());
            shardMap.put("appliedIndex", changeLogProcessor.getAppliedIndex(shardId));
            shardMap.put("slaveStatus", getSlaveStatus(shard));
            shardMap.put("role", shard.getLocalRole());
            shards.put(shardId, shardMap);
        }
        return shards;
    }

    private Map getShardManagerStatus() {
        Map<Object, Object> map = new LinkedHashMap<>();
        ShardManagerServer shardManagerServer = GondolaApplication.getShardManagerServer();
        if (shardManagerServer != null) {
            ShardManager shardManager = shardManagerServer.getShardManager();
            map.put("shardManagerServer", shardManagerServer);
            map.put("shardManager", shardManager);
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
