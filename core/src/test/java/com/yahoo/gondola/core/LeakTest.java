/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.core;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.RoleChangeEvent;

import org.apache.log4j.PropertyConfigurator;
import org.testng.annotations.Test;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.net.URL;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class LeakTest {
    Gondola g1;
    Gondola g2;
    static boolean waitForLeader = true;

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("fast")) {
            waitForLeader = false;
        }

        PropertyConfigurator.configure("conf/leaktest.log4j.properties");
        new LeakTest();
    }

    public LeakTest() throws Exception {
        Config config = new Config(new File("conf/leaktest.conf"));
        g1 = new Gondola(config, "host1");
        g2 = new Gondola(config, "host2");

        Set<String> original = getThreads();
        System.out.println("Original threads: " + original);

        while (true) {
            oneIteration();

            // Determine the diff
            Set<String> current = getThreads();
            current.removeAll(original);
            current.remove("Abandoned connection cleanup thread");
            current.remove("pool-1-thread-1");
            current.remove("Attach Listener");
            
            if (current.size() != 0) {
                System.out.printf("\nRemaining threads after stop(): %s\n", current);
            }
            System.out.print(".");
        }
    }

    public void oneIteration() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Consumer<RoleChangeEvent> listener1 = getRoleChangeEventConsumer(latch);
        Consumer<RoleChangeEvent> listener2 = getRoleChangeEventConsumer(latch);
        g1.registerForRoleChanges(listener1);
        g2.registerForRoleChanges(listener2);

        // Start the threads
        g1.start();
        g2.start();

        // Block until a leader has been elected
        if (waitForLeader) {
            latch.await();
        }

        // Stop the threads and discard all resources
        boolean stopStatus = g1.stop();
        stopStatus = g2.stop() && stopStatus;
        if (!stopStatus) {
            // Wait so this can be debugged
            System.out.println("An error was encountered while stopping the Gondola instance. ");
            System.out.println("The test has been paused so the stop error can be investigated.");
            Thread.currentThread().join();
        }
        g1.unregisterForRoleChanges(listener1);
        g2.unregisterForRoleChanges(listener2);
    }

    Consumer<RoleChangeEvent> getRoleChangeEventConsumer(CountDownLatch latch) {
        return e -> {
            if (e.leader != null) {
                latch.countDown();
            }
        };
    }

    Set<String> getThreads() {
        Set<String> set = new HashSet<>();
        ThreadInfo[] threads = ManagementFactory.getThreadMXBean().dumpAllThreads(true, true);
        for (ThreadInfo thread : threads) {
            set.add(thread.getThreadName());
        }
        return set;
    }
}
