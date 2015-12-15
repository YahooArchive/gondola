/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.rc;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Gondola Remote Control. This class is used for unit testing.
 * An Rc instance mocks storage, networking, and the clock, which allows the test to
 * control how things are stored, how messages are delivered, and time-related activities.
 *
 * An Rc instance can be reset so it can be reused for many test cases.
 */
public class GondolaRc {
    static Logger logger = LoggerFactory.getLogger(GondolaRc.class);

    Map<Integer, MemberRc> members = new HashMap<>();
    List<Gondola> gondolas = new ArrayList<>();
    List<Gondola> extras = new ArrayList<>();
    Config config;

    /**
     * Creates all gondola instances in shard1. 
     */
    public GondolaRc() throws Exception {
        config = new Config(new File("conf/gondola-rc.conf"));

        // Create all gondola instances involved with the shard "shard1".
        for (String h : config.getHostIds()) {
            for (String s : config.getShardIds(h)) {
                if (s.equals("shard1")) {
                    gondolas.add(new Gondola(config, h));
                }
            }
        }
    }

    /**
     * Add other gondola instances to this rc. Currently only used to help advance the clock
     * of these extra instances. The gondola instance will be forgotten when stop() is called.
     * Neither start() nor stop() will not be called on the supplied gondola instance.
     *
     * @param gondola non-null gondola instance.
     */
    public void add(Gondola gondola) {
        extras.add(gondola);
    }

    public void start() throws Exception {
        for (Gondola g : gondolas) {
            g.start();
            MemberRc member = new MemberRc(this, g);
            members.put(member.getMemberId(), member);
        }
    }

    /**
     * Does not stop the extra gondola instances.
     */
    public void stop() {
        for (Gondola g : gondolas) {
            boolean status = g.stop();
            if (!status) {
                logger.warn("Failed to properly stop Gondola instance for host " + g.getHostId());
            }
        }
        extras.clear();
        members.clear();
    }

    public Config getConfig() {
        return config;
    }

    /**
     * Reinitializes the members after changing the contents of storage.
     */
    public void resetMembers() throws Exception{
        for (MemberRc m : members.values()) {
            m.reset();
        }
    }

    /**
     * Virtual sleep.
     */
    public void sleep(int ms) throws Exception {
        // Only need to use one of the gondola instances to sleep
        gondolas.get(0).getClock().sleep(ms);
    }

    /**
     * Advances virtual time.
     */
    public void tick(int ms) {
        for (Gondola g : gondolas) {
            ((RcClock) g.getClock()).tick(ms);
        }
        for (Gondola g : extras) {
            ((RcClock) g.getClock()).tick(ms);
        }

        // Let things progress after advancing the clock
        try {
            Thread.sleep(Math.min(1000, ms));
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Returns the current virtual time.
     */
    public long getCurrentTime() {
        // All clocks should be the same so return the value from the first one
        return ((RcClock) gondolas.get(0).getClock()).now();
    }

    /**
     * Shows a summary of the internal state of all members.
     */
    public void showSummaries() {
        for (MemberRc m : members.values()) {
            m.showSummary();
        }
    }

    /**
     * Returns a rc member, which is a wrapper on a Gondola Member.
     *
     * @return null if memberId does not exist.
     */
    public MemberRc getMember(int memberId) {
        return members.get(memberId);
    }

    public boolean supportsPauseDelivery() {
        return gondolas.get(0).getNetwork() instanceof RcNetwork;
    }
}
