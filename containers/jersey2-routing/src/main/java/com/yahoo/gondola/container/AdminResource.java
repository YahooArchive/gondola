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

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.ejb.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

@Path("/gondola/v1/")
@Produces("application/json")
@Singleton
public class AdminResource {

    static Logger logger = LoggerFactory.getLogger(GondolaAdminResource.class);
    AdminClient client;
    Config config;
    ShardManagerClient shardManagerClient = GondolaApplication.getShardManagerServer().getShardManager().getShardManagerClient();

    public AdminResource() {
        Gondola gondola = GondolaApplication.getRoutingFilter().getGondola();
        this.config = gondola.getConfig();
        client = AdminClient.getInstance(config, "adminResource");
    }


    public void setServiceName(String serviceName) throws AdminClient.AdminException {
        client.setServiceName(serviceName);
    }

    public String getServiceName() throws AdminClient.AdminException {
        return client.getServiceName();
    }

    public Config getConfig() throws AdminClient.AdminException {
        return client.getConfig();
    }

    public void setConfig(File configFile) throws AdminClient.AdminException {
        client.setConfig(configFile);
    }

    public void splitShard(String fromShardId, String toShardId)
        throws AdminClient.AdminException, InterruptedException {
        client.splitShard(fromShardId, toShardId);
    }

    @POST
    @Path("/assignBuckets")
    public void assignBuckets(@QueryParam("lowerBound") int lowerBound, @QueryParam("upperBound") int upperBound,
                              @QueryParam("fromShardId") String fromShardId, @QueryParam("toShardId") String toShardId)
        throws InterruptedException, AdminClient.AdminException {
        client.assignBuckets(lowerBound, upperBound, fromShardId, toShardId);
    }

    public void mergeShard(String fromShardId, String toShardId)
        throws AdminClient.AdminException, InterruptedException {
        client.mergeShard(fromShardId, toShardId);
    }

    public void enable(AdminClient.Target target, String targetId) throws AdminClient.AdminException {
        client.enable(target, targetId);
    }

    public void disable(AdminClient.Target target, String targetId) throws AdminClient.AdminException {
        client.disable(target, targetId);
    }

    public void enableTracing(AdminClient.Target target, String targetId) throws AdminClient.AdminException {
        client.enableTracing(target, targetId);
    }

    public void disableTracing(AdminClient.Target target, String targetId) throws AdminClient.AdminException {
        client.disableTracing(target, targetId);
    }

    @Path("/setSlave")
    @POST
    public Map setSlave(@QueryParam("shardId") String shardId, @QueryParam("masterShardId") String masterShardId) {
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
}
