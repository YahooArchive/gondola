/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;


import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.container.client.ShardManagerClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.NotAllowedException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

@Path("/gondola/v1/")
@Produces("application/json")
@Singleton
public class AdminResource {

    private static Logger logger = LoggerFactory.getLogger(GondolaAdminResource.class);
    private static AdminClient client;
    private static Config config;
    private static ShardManagerClient shardManagerClient;

    public AdminResource() {
        Gondola gondola = GondolaApplication.getRoutingFilter().getGondola();
        config = gondola.getConfig();
        client = AdminClient.getInstance(config, "adminResource");
        ShardManagerServer shardManagerServer = GondolaApplication.getShardManagerServer();
        if (shardManagerServer != null) {
            shardManagerClient = GondolaApplication.getShardManagerServer().getShardManager().getShardManagerClient();
        }
    }

    @POST
    @Path("/assignBuckets")
    public void assignBuckets(@QueryParam("lowerBound") int lowerBound, @QueryParam("upperBound") int upperBound,
                              @QueryParam("fromShardId") String fromShardId, @QueryParam("toShardId") String toShardId)
        throws InterruptedException, AdminClient.AdminException {
        client.assignBuckets(lowerBound, upperBound, fromShardId, toShardId);
    }

    @Path("/setSlave")
    @POST
    public Map setSlave(@QueryParam("shardId") String shardId, @QueryParam("masterShardId") String masterShardId) {
        if (shardManagerClient == null) {
            throw new NotAllowedException("The operation only supported in multi-shard mode");
        }
        Map<Object, Object> map = new LinkedHashMap<>();
        try {
            shardManagerClient.startObserving(shardId, masterShardId, 5000);
            map.put("success", true);
        } catch (ShardManagerProtocol.ShardManagerException | InterruptedException e) {
            map.put("success", false);
            map.put("reason", e.getMessage());
        }
        return map;
    }

    @Path("/unsetSlave")
    @POST
    public Map unsetSlave(@QueryParam("shardId") String shardId, @QueryParam("masterShardId") String masterShardId) {
        Map<Object, Object> map = new LinkedHashMap<>();
        if (shardManagerClient == null) {
            throw new NotAllowedException("The operation only supported in multi-shard mode");
        }
        try {
            shardManagerClient.stopObserving(shardId, masterShardId, 5000);
            map.put("success", true);
        } catch (ShardManagerProtocol.ShardManagerException | InterruptedException e) {
            map.put("success", false);
            map.put("reason", e.getMessage());
        }
        return map;
    }


    @Path("/setLeader")
    @POST
    public Map setLeader(@QueryParam("hostId") String hostId, @QueryParam("shardId") String shardId) {
        return client.setLeader(hostId, shardId);
    }

    @Path("/hostStatus")
    @GET
    public Map getHostStatus(@QueryParam("hostId") String hostId) {
        return client.getHostStatus(hostId);
    }

    @Path("/serviceStatus")
    @GET
    public Map getServiceStatus() {
        return client.getServiceStatus();
    }

    @Path("/enableSite")
    @POST
    public void enableSite(@QueryParam("siteId") String siteId, @QueryParam("enable") boolean enable)
        throws AdminClient.AdminException {
        client.enable(AdminClient.Target.SITE, siteId, enable);
    }

    @Path("/enableHost")
    @POST
    public void enableHost(@QueryParam("hostId") String hostId, @QueryParam("enable") boolean enable)
        throws AdminClient.AdminException {
        client.enable(AdminClient.Target.HOST, hostId, enable);
    }
}
