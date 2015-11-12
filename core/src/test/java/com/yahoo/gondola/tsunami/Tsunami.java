/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.tsunami;

import com.yahoo.gondola.Config;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * This test creates a 3-server cluster and randomly kills and restarts the servers.
 * On each server are N writer threads, each thread committing their thread id and a sequence number.
 * The writers to the leader will succeed while the writers on the non-leaders will all fail.
 * The writers will retry a command until it succeeds.
 * <p>
 * A verifier thread reads the committed commands from every server and verifies that the commands are
 * read in sequence. Duplicate commands are allowed. But missing commands are flagged as a major error.
 * The commands are read from each server in round-robin fashion.
 * <p>
 * The test runs three phases continuously.
 * <li> Safe - the N writers are allowed to write without interruption for N seconds.
 * <li> Nasty - the servers are randonly restarted to induce errors.
 * <li> Sync - the writers are stopped until the reader has caught up.
 * <p>
 * For even more stress, the storage system is wrapped around NastyStorage, which introduces random delays
 * and exception to storage operations.
 */
public class Tsunami {
    static Logger logger = LoggerFactory.getLogger(Tsunami.class);

    static Config config;
    List<String> hostIds = Arrays.asList("host1:1099", "host2:1100", "host3:1101");
    AgentClient[] agents = new AgentClient[hostIds.size()];

    ExecutorService executorService = Executors.newCachedThreadPool();

    ReentrantLock lock = new ReentrantLock();

    // This map contains the last index written by every writer. Used to verify consecutive indices.
    // map<writer-id, index>
    Map<String, Integer> lastIndices = new HashMap<>();

    AtomicInteger writes = new AtomicInteger();
    AtomicInteger reads = new AtomicInteger();
    AtomicInteger errors = new AtomicInteger();

    static int numWriters = 5;

    // Used in sync phase
    int lastWrittenIndex = 0;
    int lastReadIndex = 0;

    // The instance of gondola that the verifier is waiting to get a committed command
    int verifyWaitingFor = 0;
    int verifyWaitingForIndex = 0;

    // When no read has succeeded for a while, stop killing gondola instances
    long lastReadTs = 0;

    public enum Phase {
        SYNC,  // Writers stop until the reader has caught up
        SAFE,  // The instances are not restarted during this phase
        NASTY  // Instances are randomly restarted
    }

    // Current phase
    Phase phase = Phase.SYNC;

    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure("conf/log4j.properties");
        config = new Config(new File("conf/gondola-tsunami.conf"));

        // Process command line arguments
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("w", true, "Number of writers per member, default = " + numWriters);
        options.addOption("h", false, "help");
        CommandLine commandLine = parser.parse(options, args);
        if (commandLine.hasOption("h")) {
            new HelpFormatter().printHelp("Tsunami Test", options);
            return;
        }
        if (commandLine.hasOption("w")) {
            numWriters = Integer.parseInt(commandLine.getOptionValue("w"));
        }
        new Tsunami();
    }

    public Tsunami() throws Exception {
        setup();

        // Print status thread
        executorService.execute(() -> {
                    while (true) {
                        logger.info("writes: {}, reads: {}, errors: {}, waiting for index={} on {}",
                                writes.get(), reads.get(), errors.get(), verifyWaitingForIndex, agents[verifyWaitingFor].hostId);
                        logger.info("  " + Arrays.stream(agents)
                                .map(agent -> agent.hostId + ": up=" + agent.up)
                                .collect(Collectors.joining(", ")));
                        logger.info("  lastWrite: {}, lastRead: {}", lastWrittenIndex, lastReadIndex);
                        sleep(10000);
                    }
                }
        );

        executorService.execute(new Killer());
        executorService.execute(new Verifier());

        // Initialize writer threads. numWriter writers per gondola
        for (int a = 0; a < agents.length; a++) {
            AgentClient agent = agents[a];
            for (int w = 0; w < numWriters; w++) {
                String writerId = String.format("%c%d", (char) ('A' + a), w);

                executorService.execute(new Writer(writerId, agent.hostname, agent.gondolaCc.port));
            }
        }
    }

    void setup() throws Exception {
        // Create gondola instances
        for (int i = 0; i < hostIds.size(); i++) {
            String[] split = hostIds.get(i).split(":");
            String hostId = split[0];
            String cliPort = split[1];
            InetSocketAddress addr = config.getAddressForHost(hostId);

            // Initialize the instance
            agents[i] = new AgentClient(hostId, addr.getHostName(), 1200,
                    new CliClient(addr.getHostName(), Integer.parseInt(cliPort), 60000)); // 1m timeout
            //agents[i].createInstance();
        }
    }

    static void sleep(int delayMs) {
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            logger.error("", e);
        }
    }

    /**
     * ************* threads ***************
     */

    /**
     * This thread continuously tries to write a command into the log. If a failure occurs, it waits for a small delay
     * and then tries again. The command contains the writer's id and a monotonically incrementing counter specific to
     * this writer. The verifier confirms that the counter increases consecutively.
     */
    class Writer implements Runnable {
        String id;
        CliClient cliClient;

        public Writer(String id, String hostname, int cliPort) throws Exception {
            this.id = id;
            cliClient = new CliClient(hostname, cliPort, 0); // no timeout
            lastIndices.put(id, 0);
            Thread.currentThread().setName("Writer");
        }

        @Override
        public void run() {
            boolean lastSuccess = false;
            for (int index = 1; ; index++) {
                String command = String.format("%s-%d", id, index);

                // Keep retrying until the write succeeds
                while (true) {
                    try {
                        // Wait for the reader to catch up
                        while (phase == Phase.SYNC) {
                            Thread.sleep(1000);
                        }

                        if (Math.random() < .001) {
                            cliClient.forceLeader(1000);
                        }

                        // Write the command
                        lastWrittenIndex = Math.max(lastWrittenIndex, cliClient.commit(command));
                        //logger.info("Wrote {}", command);
                        writes.incrementAndGet();
                        lastSuccess = true;
                        sleep((int) (Math.random() * 500));
                        break;
                    } catch (Exception e) {
                        if (lastSuccess) {
                            // Show the error only if the last command succeeded, to cut down on output noise
                            logger.info("Failed to write {}: {}", command, e.getMessage());
                            lastSuccess = false;
                        }
                        errors.incrementAndGet();
                        sleep(1000);
                    }
                }
            }
        }
    }

    /**
     * This thread reads the committed commands from each member and checks that they're all
     * identical.
     * It also does a second check to make sure that the counters from each writer is monotonically increasing.
     */
    class Verifier implements Runnable {
        Verifier() {
            Thread.currentThread().setName("Verifier");
        }

        @Override
        public void run() {
            CliClient[] clients = new CliClient[agents.length];
            try {
                for (int i = 0; i < clients.length; i++) {
                    clients[i] = new CliClient(agents[i].hostname, agents[i].gondolaCc.port, 60000);
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }

            for (int index = 1; ; index++) {
                while (true) {
                    try {
                        verifyWaitingFor = 0;
                        verifyWaitingForIndex = index;
                        String command = clients[0].getCommand(index, 30000);
                        lastReadTs = System.currentTimeMillis();
                        //logger.info("[{}] Read index={}: {}", agents[0].hostId, index, command);

                        // Verify all three hosts are the same
                        for (int i = 1; i < agents.length; i++) {
                            verifyWaitingFor = i;
                            verifyWaitingForIndex = index;
                            String command2 = clients[i].getCommand(index, 30000);
                            lastReadTs = System.currentTimeMillis();

                            if (!command.equals(command2)) {
                                logger.error("Command at index {} from {}={} does not match command from {}={}. lastIndices={}",
                                        index, agents[0].hostId, command, agents[i].hostId, command2, lastIndices);
                                System.exit(1);
                            }
                        }

                        // Verify that the indexes are monotonically increasing
                        if (!command.equals("")) {
                            String[] parts = command.split("-");
                            String writerId = parts[0];
                            int ix = Integer.parseInt(parts[1]);

                            int lastIndex = lastIndices.get(writerId);
                            // Allow duplicates (for now until we can determine if can be avoided)
                            if (ix == lastIndex) {
                                logger.warn("Command at index {} for writer {}={} is a duplicate",
                                        index, writerId, ix);
                            } else if (ix == lastIndex + 1) {
                                lastIndices.put(writerId, ix);
                            } else {
                                logger.error("Command at index {} for writer {}={} is not one bigger than last index {}. lastIndices={}",
                                        index, writerId, ix, lastIndex, lastIndices);
                                System.exit(1);
                            }
                        }
                        reads.incrementAndGet();
                        lastReadIndex = index;
                        break;
                    } catch (EOFException e) {
                        logger.error("EOF: Failed to execute: {}", e.getMessage());
                        errors.incrementAndGet();
                    } catch (Exception e) {
                        String msg = e.getMessage();
                        if (msg.indexOf("Timeout") >= 0 || msg.indexOf("ERROR: Unhandled") >= 0) {
                            logger.error(msg);
                        } else {
                            logger.error(e.getMessage(), e);
                        }
                        errors.incrementAndGet();
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e1) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }
            }
        }
    }

    /**
     * This thread randomly kills and restarts members.
     */
    class Killer implements Runnable {
        static final int SYNC_PERIOD = 1000;
        static final int NASTY_PERIOD = 60000;
        static final int SAFE_PERIOD = 1000;

        Killer() {
            Thread.currentThread().setName("Killer");
        }

        @Override
        public void run() {
            long phaseTimeoutTs = 0;

            while (true) {
                if (System.currentTimeMillis() >= phaseTimeoutTs) {
                    switch (phase) {
                        case SAFE:
                            phase = Phase.NASTY;
                            logger.info("--- " + phase + " phase ---");
                            enableNastyStorage(true);
                            phaseTimeoutTs = System.currentTimeMillis() + NASTY_PERIOD;
                            break;
                        case NASTY:
                            phase = Phase.SYNC;
                            logger.info("--- " + phase + " phase ---");
                            phaseTimeoutTs = System.currentTimeMillis() + SYNC_PERIOD;
                            break;
                        case SYNC:
                            // Restart instances
                            for (int i = 0; i < agents.length; i++) {
                                if (!agents[i].gondolaCc.isOperational()) {
                                    logger.info("Creating instance {}", agents[i].hostId);
                                    agents[i].createInstance();
                                }
                            }
                            enableNastyStorage(false);
                            if (lastReadIndex >= lastWrittenIndex) {
                                phase = Phase.SAFE;
                                logger.info("--- " + phase + " phase ---");
                                phaseTimeoutTs = System.currentTimeMillis() + SAFE_PERIOD;
                            } else {
                                phaseTimeoutTs = System.currentTimeMillis() + SYNC_PERIOD;
                            }
                            break;
                    }
                }
                sleep((int) (Math.random() * 5000));

                int r = (int) (Math.random() * agents.length);
                AgentClient agent = agents[r];
                long lastReadTime = System.currentTimeMillis() - lastReadTs;

                if (agent.up) {
                    if (lastReadTime >= 60000 && !agent.gondolaCc.isOperational()) {
                        logger.info("Creating instance {} which appears to be down", agent.hostId);
                        agent.createInstance();
                        lastReadTs = System.currentTimeMillis();
                    } else if (phase == Phase.NASTY && Math.random() > .8) {
                        logger.info("Killing instance {}", agent.hostId);

                        // Now ask the agent to do it again, just in case
                        agent.destroyInstance();
                    }
                } else {
                    logger.info("Creating instance {}", agent.hostId);
                    agent.createInstance();
                }
            }
        }

        void enableNastyStorage(boolean on) {
            // Enable nasty storage
            for (int i = 0; i < agents.length; i++) {
                try {
                    agents[i].gondolaCc.enableNastyStorage(on);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }
}
