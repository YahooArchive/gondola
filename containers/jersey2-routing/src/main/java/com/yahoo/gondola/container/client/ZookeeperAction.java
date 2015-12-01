/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * The type Zookeeper action.
 */
public class ZookeeperAction {

    /**
     * The enum Action.
     */
    public enum Action {
        NOOP, START_SLAVE, STOP_SLAVE, MIGRATE_1, MIGRATE_2, MIGRATE_3
    }

    ;
    public Action action = Action.NOOP;
    public int memberId;
    public List<Object> args;

    Logger logger = LoggerFactory.getLogger(ZookeeperAction.class);

    /**
     * Parse args args.
     *
     * @return the args
     */
    public Args parseArgs() {
        try {
            Args argsObj = new Args();
            switch (action) {
                case NOOP:
                    break;
                case START_SLAVE:
                case STOP_SLAVE:
                    argsObj.fromShard = (String) args.get(0);
                    argsObj.toShard = (String) args.get(1);
                    argsObj.timeoutMs = Long.parseLong(String.valueOf(args.get(2)));
                    break;
                case MIGRATE_1:
                    argsObj.rangeStart = (Integer) args.get(0);
                    argsObj.rangeStop = (Integer) args.get(1);
                    argsObj.fromShard = (String) args.get(2);
                    argsObj.toShard = (String) args.get(3);
                    argsObj.timeoutMs = Long.parseLong(String.valueOf(args.get(4)));
                    break;
                case MIGRATE_2:
                case MIGRATE_3:
                    argsObj.rangeStart = (Integer) args.get(0);
                    argsObj.rangeStop = (Integer) args.get(1);
                    argsObj.fromShard = (String) args.get(2);
                    argsObj.toShard = (String) args.get(3);
                    argsObj.complete = (Boolean) args.get(4);
                    break;
            }
            return argsObj;
        } catch (Exception e) {
            logger.error("Cannot parse args={}, message={}", args, e.getMessage());
            throw new IllegalStateException(e);
        }
    }

    /**
     * The type Args.
     */
    public static class Args {

        public String fromShard;
        public String toShard;
        public long timeoutMs;
        public int rangeStart;
        public int rangeStop;
        public boolean complete;
    }

    @Override
    public String toString() {
        return "ZookeeperAction{"
               + "action=" + action
               + ", memberId=" + memberId
               + ", args=" + args
               + '}';
    }
}
