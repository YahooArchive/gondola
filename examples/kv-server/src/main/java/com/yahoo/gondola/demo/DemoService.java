/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.demo;

import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.RoleChangeEvent;
import com.yahoo.gondola.container.ChangeLogProcessor;
import com.yahoo.gondola.container.RoutingService;
import com.yahoo.gondola.container.spi.RoutingHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * The core business logic of demo service.
 */
public class DemoService extends RoutingService {

    private static Logger logger = LoggerFactory.getLogger(DemoService.class);
    private Map<String, String> entries = new ConcurrentHashMap<>();
    private String hostId;
    private DemoRoutingHelper demoRoutingHelper;

    /**
     * Instantiates a new Routing service.
     *
     * @param gondola the gondola
     */
    public DemoService(Gondola gondola) {
        super(gondola);
        hostId = gondola.getHostId();
        demoRoutingHelper = new DemoRoutingHelper(gondola.getHostId(), gondola.getConfig());
        gondola.registerForRoleChanges(listener);
    }

    Consumer<RoleChangeEvent> listener = crevt -> {
        switch (crevt.newRole) {
            case CANDIDATE:
                logger.info("[{}] Current role: CANDIDATE", gondola.getHostId());
                break;
            case LEADER:
                logger.info("[{}] Current role: LEADER", gondola.getHostId());
                break;
            case FOLLOWER:
                logger.info("[{}] Current role: FOLLOWER", gondola.getHostId());
                break;
        }
    };


    /**
     * Returns the value stored at the specified key.
     *
     * @param key            the key
     * @param servletRequest the servlet request
     * @return The non-null value of the key
     * @throws NotLeaderException the not leader exception
     * @throws NotFoundException  the not found exception
     */
    public String getValue(String key, ContainerRequestContext servletRequest)
        throws NotLeaderException, NotFoundException {
        if (!isLeader(getShardId(servletRequest))) {
            throw new NotLeaderException();
        }
        if (!entries.containsKey(key)) {
            throw new NotFoundException();
        }
        String value = entries.get(key);
        logger.info(String.format("[%s] Get key %s: %s", this.hostId, key, value));
        return value;
    }

    /**
     * Commits the entry to Raft log. The entries map is not updated; it is updated by the Replicator thread.
     *
     * @param key            The non-null key
     * @param value          The non-null value
     * @param servletRequest the servlet request
     * @throws NotLeaderException the not leader exception
     */
    public void putValue(String key, String value, ContainerRequestContext servletRequest) throws NotLeaderException {
        if (key.indexOf(" ") >= 0) {
            throw new IllegalArgumentException("The key must not contain spaces");
        }
        try {
            byte[] bytes = (key + " " + value).getBytes(); // TODO implement better separator
            writeLog(getShardId(servletRequest), bytes);
            logger.info(String.format("[%s] Put key %s=%s", hostId, key, value));
        } catch (com.yahoo.gondola.NotLeaderException e) {
            logger.info(String.format("Failed to put %s/%s because not a leader", key, value));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ChangeLogProcessor.ChangeLogConsumer provideChangeLogConsumer() {
        return (shardId, command) -> {
            String[] pair = command.getString().split(" ", 2);
            if (pair.length == 2) {
                entries.put(pair[0], pair[1]);
            }
        };
    }

    @Override
    public RoutingHelper provideRoutingHelper() {
        return demoRoutingHelper;
    }

    @Override
    public void ready(String shardId) {
        logger.info("[{}] {} ready for serving", gondola.getHostId(), shardId);
    }

    /**
     * The type Not leader exception.
     */
    public class NotLeaderException extends Exception {

    }

    /**
     * The type Not found exception.
     */
    public class NotFoundException extends Exception {

    }
}
