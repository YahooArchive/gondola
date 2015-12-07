/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
    static ObjectMapper objectMapper = Utils.getObjectMapperInstance();
    AdminClient client;

    public AdminResource() {
        // TODO: refine admin client interface
        Gondola gondola = GondolaApplication.getRoutingFilter().getGondola();
        client = new AdminClient(Utils.getRegistryConfig(gondola.getConfig()).attributes.get("serviceName"),
                                 GondolaApplication.getShardManagerClient(),
                                 gondola.getConfig(),
                                 new ConfigWriter(gondola.getConfig().getFile()),
                                 new GondolaAdminClient(gondola.getConfig()));
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

    @Path("/setLeader")
    @POST
    public Map setLeader(@QueryParam("hostId") String hostId, @QueryParam("shardId") String shardId) {
        return client.setLeader(hostId, shardId);
    }

    @Path("/getHostStatus")
    @GET
    public Map getHostStatus(@QueryParam("hostId") String hostId) {
        return client.getHostStatus(hostId);
    }

    @Path("/getServiceStatus")
    @GET
    public Map getServiceStatus() {
        return client.getServiceStatus();
    }
}
