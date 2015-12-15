/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.codahale.metrics.Timer;
import com.yahoo.gondola.Command;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.GondolaException;
import com.yahoo.gondola.RoleChangeEvent;
import com.yahoo.gondola.Shard;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * The type Routing service.
 */
public abstract class RoutingService {

    protected String hostId;
    protected String shardId;
    protected int memberId;
    protected Gondola gondola;
    private Shard shard;
    private List<Consumer<RoleChangeEvent>> eventCallbacks = new ArrayList<>();
    private Timer commitTimer;


    // TODO: use dependency injection to hide gondola instance from user app.
    public RoutingService(Gondola gondola, String shardId) {
        this.gondola = gondola;
        shard = gondola.getShard(shardId);
        hostId = gondola.getHostId();
        this.shardId = shardId;
        memberId = shard.getLocalMember().getMemberId();
        commitTimer = GondolaApplication.MyMetricsServletContextListener.METRIC_REGISTRY.timer("LogWriter");
    }

    /**
     * Register callback for getting all container events.
     *
     * @param consumer
     */
    public void registerEventHandler(Consumer<RoleChangeEvent> consumer) {
        gondola.registerForRoleChanges(consumer);
        eventCallbacks.add(consumer);
    }

    /**
     * Provide changeLog consumer
     */
    public abstract ChangeLogProcessor.ChangeLogConsumer provideChangeLogConsumer();

    /**
     * Called by container when the shard is ready for serving.
     */
    public abstract void ready();

    /**
     * Write log.
     *
     * @param bytes the bytes
     * @throws GondolaException   thrown if the local member is not the leader
     * @throws InterruptedException the interrupted exception
     */
    public void writeLog(byte[] bytes)
        throws GondolaException, InterruptedException {
        Command command = shard.checkoutCommand();
        Timer.Context time = commitTimer.time();
        command.commit(bytes, 0, bytes.length);
        time.stop();
    }
    /**
     * Is leader boolean.
     *
     * @return the boolean
     */
    public boolean isLeader() {
        return shard.getLocalMember().isLeader();
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
