/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.demo;

import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.GondolaException;
import com.yahoo.gondola.RoleChangeEvent;
import com.yahoo.gondola.container.ChangeLogProcessor;
import com.yahoo.gondola.container.RoutingService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * The core business logic of demo service.
 */
public class DemoService extends RoutingService {

    private static Logger logger = LoggerFactory.getLogger(DemoService.class);
    private Map<String, String> entries = new ConcurrentHashMap<>();

    /**
     * Instantiates a new Routing service.
     */
    public DemoService(Gondola gondola, String shardId) {
        super(gondola, shardId);
        registerEventHandler(listener);
    }

    Consumer<RoleChangeEvent> listener = crevt -> {
        switch (crevt.newRole) {
            case CANDIDATE:
                logger.info("[{}] Current role: CANDIDATE", hostId);
                break;
            case LEADER:
                logger.info("[{}] Current role: LEADER", hostId);
                break;
            case FOLLOWER:
                logger.info("[{}] Current role: FOLLOWER", hostId);
                break;
        }
    };


    /**
     * Returns the value stored at the specified key.
     *
     * @param key the key
     * @return The non-null value of the key
     * @throws NotLeaderException the not leader exception
     * @throws NotFoundException  the not found exception
     */
    public String getValue(String key)
        throws NotLeaderException, NotFoundException {
        if (!isLeader()) {
            throw new NotLeaderException();
        }
        if (!entries.containsKey(key)) {
            logger.info("[{}] Get key {}, but data not found", this.hostId, key);
            throw new NotFoundException();
        }
        String value = entries.get(key);
        logger.info("[{}] Get key {}={}", this.hostId, key, value);
        return value;
    }

    /**
     * Commits the entry to Raft log. The entries map is not updated; it is updated by the Replicator thread.
     *
     * @param key   The non-null key
     * @param value The non-null value
     * @throws NotLeaderException the not leader exception
     */
    public void putValue(String key, String value) throws NotLeaderException {
        if (key.contains(" ")) {
            throw new IllegalArgumentException("The key must not contain spaces");
        }
        try {
            byte[] bytes = (key + " " + value).getBytes(); // TODO implement better separator
            writeLog(bytes);
            logger.info("[{}] Put key {}={}", hostId, key, value);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (GondolaException e) {
            logger.info("Failed to put {}/{} reason={}", key, value, e.getCode());
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
    public void ready() {
        logger.info("[{}-{}] {} ready for serving", hostId, memberId, shardId);
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
