/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

@Path("/gondola/shardManager/v1/")
public class HttpShardManagerServer implements ShardManagerServer {

    ShardManager shardManager;
    Lock lock = new ReentrantLock();

    public HttpShardManagerServer(ShardManager shardManager) {
        this.shardManager = shardManager;
    }

    @Override
    public void stop() {

    }

    @Override
    public ShardManager getShardManager() {
        return null;
    }

    @Override
    public Map getStatus() {
        return null;
    }

    @POST
    @Path("/startObserving")
    public void startObserving(@QueryParam("shardId") String shardId,
                               @QueryParam("observedShardId") String observedShardId,
                               @QueryParam("timeoutMs") long timeoutMs)
        throws ShardManagerProtocol.ShardManagerException, InterruptedException {
        lock.lock();
        try {
            shardManager.startObserving(shardId, observedShardId, timeoutMs);
        } finally {
            lock.unlock();
        }
    }

    @GET
    @Path("/waitSlavesSynced")
    public boolean waitSlavesSynced(@QueryParam("shardId") String shardId, @QueryParam("timeoutMs") long timeoutMs)
        throws ShardManagerProtocol.ShardManagerException, InterruptedException {
        lock.lock();
        try {
            return shardManager.waitSlavesSynced(shardId, timeoutMs);
        } finally {
            lock.unlock();
        }
    }

    @POST
    @Path("/migrateBuckets")
    public void migrateBuckets(@QueryParam("lowerBound") int lowerBound, @QueryParam("upperBound") int upperBound,
                               @QueryParam("fromShardId") String fromShardId,
                               @QueryParam("toShardId") String toShardId, @QueryParam("timeoutMs") long timeoutMs)
        throws ShardManagerProtocol.ShardManagerException {
        lock.lock();
        try {
            shardManager.migrateBuckets(Range.closed(lowerBound, upperBound), fromShardId, toShardId, timeoutMs);
        } finally {
            lock.unlock();
        }
    }

    @GET
    @Path("waitSlavesApproaching")
    public boolean waitSlavesApproaching(@QueryParam("shardId") String shardId, @QueryParam("timeoutMs") long timeoutMs)
        throws ShardManagerProtocol.ShardManagerException, InterruptedException {
        lock.lock();
        try {
            return shardManager.waitSlavesApproaching(shardId, timeoutMs);
        } finally {
            lock.unlock();
        }
    }

    @POST
    @Path("stopObserving")
    public void stopObserving(@QueryParam("shardId") String shardId, @QueryParam("masterShardId") String masterShardId,
                              @QueryParam("timeoutMs") long timeoutMs)
        throws ShardManagerProtocol.ShardManagerException, InterruptedException {
        lock.lock();
        try {
            shardManager.stopObserving(shardId, masterShardId, timeoutMs);
        } finally {
            lock.unlock();
        }
    }

    @POST
    @Path("setBuckets")
    public void setBuckets(@QueryParam("lowerBound") int lowerBound, @QueryParam("upperBound") int upperBound,
                           @QueryParam("fromShardId") String fromShardId,
                           @QueryParam("toShardId") String toShardId,
                           @QueryParam("migrationComplete") boolean migrationComplete) {
        lock.lock();
        try {
            shardManager.setBuckets(Range.closed(lowerBound, upperBound), fromShardId, toShardId, migrationComplete);
        } finally {
            lock.unlock();
        }
    }
}
