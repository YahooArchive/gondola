/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.yahoo.gondola.Command;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.NotLeaderException;
import com.yahoo.gondola.container.spi.RoutingHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * The type Routing service.
 */
public abstract class RoutingService {

    protected Gondola gondola;
    private List<Consumer<Event>> eventCallbacks = new ArrayList<>();

    /**
     * Instantiates a new Routing service.
     *
     * @param gondola the gondola
     */
    public RoutingService(Gondola gondola) {
        this.gondola = gondola;
    }

    public void registerEventHandler(Consumer<Event> consumer) {
        eventCallbacks.add(consumer);
    }

    /**
     * Provide changeLog consumer
     *
     */
    public abstract ChangeLogProcessor.ChangeLogConsumer provideChangeLogConsumer();

    /**
     * Provide routing helper routing helper.
     *
     * @return the routing helper
     */
    public abstract RoutingHelper provideRoutingHelper();

    /**
     * Called by container when the shard is ready for serving.
     *
     * @param shardId the shard id
     */
    public abstract void ready(String shardId);

    /**
     * Write log.
     *
     * @param shardId the shard id
     * @param bytes   the bytes
     * @throws NotLeaderException   the not leader exception
     * @throws InterruptedException the interrupted exception
     */
    public void writeLog(String shardId, byte[] bytes)
        throws NotLeaderException, InterruptedException {
        Command command = gondola.getShard(shardId).checkoutCommand();
        command.commit(bytes, 0, bytes.length);
    }

    /**
     * Gets shard id.
     *
     * @param request the request
     * @return the shard id
     */
    public String getShardId(ContainerRequestContext request) {
        return (String) request.getProperty("shardId");
    }

    /**
     * Gets bucket id.
     *
     * @param request the request
     * @return the bucket id
     */
    public int getBucketId(ContainerRequestContext request) {
        return Integer.parseInt((String) request.getProperty("bucketId"));
    }

    /**
     * Is leader boolean.
     *
     * @param shardId the shard id
     * @return the boolean
     */
    public boolean isLeader(String shardId) {
        return gondola.getShard(shardId).getLocalMember().isLeader();
    }

    /**
     * The container event.
     */
    public static class Event {

        public Type type;

        /**
         * Event type.
         */
        public enum Type {
            ROLE_CHANGE
        }
    }
}
