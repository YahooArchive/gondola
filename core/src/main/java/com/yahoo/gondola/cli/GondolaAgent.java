/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.cli;

import com.yahoo.gondola.Config;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.exec.*;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class GondolaAgent {
    static Logger logger = LoggerFactory.getLogger(GondolaAgent.class);

    static String configFile;
    static int port;
    static Config config;
    AtomicInteger jdwpPort = new AtomicInteger(5005);

    // hostId -> process
    Map<String, Process> processMap = new ConcurrentHashMap<>();

    // hostId -> port
    Map<String, Integer> jdwpPortMap = new ConcurrentHashMap<>();

    public static void main(String[] args) throws ParseException, IOException {
        PropertyConfigurator.configure("conf/log4j.properties");
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("port", true, "Listening port");
        options.addOption("config", true, "config file");
        options.addOption("h", false, "help");
        CommandLine commandLine = parser.parse(options, args);

        if (commandLine.hasOption("help")) {
            new HelpFormatter().printHelp("GondolaAgent", options);
            return;
        }

        if (commandLine.hasOption("port")) {
            port = Integer.parseInt(commandLine.getOptionValue("port"));
        } else {
            port = 1200;
        }
        if (commandLine.hasOption("config")) {
            configFile = commandLine.getOptionValue("config");
        } else {
            configFile = "conf/gondola-sample.conf";
        }

        config = new Config(new File(configFile));

        logger.info("Initialize system, kill all gondola processes");
        new DefaultExecutor().execute(org.apache.commons.exec.CommandLine.parse("bin/gondola-local-test.sh stop"));

        new GondolaAgent(port);
    }

    public GondolaAgent(int port) throws IOException {
        ServerSocket serversocket = new ServerSocket(port);
        logger.info("Listening on port " + port);
        Socket socket;
        while ((socket = serversocket.accept()) != null) {
            new Handler(socket).start();
        }
    }

    public class Handler extends Thread {
        Socket socket;

        public Handler(Socket socket) {
            this.socket = socket;
        }

        public void run() {
            try (
                    InputStream in = socket.getInputStream();
                    OutputStream out = socket.getOutputStream();
                    BufferedReader rd = new BufferedReader(new InputStreamReader(in));
                    BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(out));
            ) {
                String line;
                String hostId;
                while ((line = rd.readLine()) != null) {
                    String[] split = line.split(" ");
                    logger.info("Received command: {}", line);
                    switch (split[0]) {
                        // create <param1> <param2> ... -> gondola.sh <param1> <param2> ....
                        case "create":
                            if (split.length >= 2) {
                                execGondola(split[1], line.substring(line.indexOf(split[1]) + split[1].length()));
                                wr.write("SUCCESS: Created " + split[1] + "\n");
                            } else {
                                wr.write("ERROR: Invalid command: " + line + "\n");
                            }
                            break;
                        // destroy <hostId>
                        case "destroy":
                            if (split.length == 2) {
                                hostId = split[1];
                                if (killGondola(hostId)) {
                                    wr.write("SUCCESS: hostId: " + hostId + " killed.\n");
                                } else {
                                    wr.write("ERROR: hostId: " + hostId + " not found.\n");
                                }
                            } else {
                                wr.write("ERROR: Invalid command: " + line + "\n");
                            }
                            break;
                        // full_destroy <hostId>
                        case "full_destroy":
                            if (split.length == 2) {
                                hostId = split[1];
                                killGondola(hostId);
                                // wait for a while
                                Thread.sleep(500);
                                // delete gondola db files
                                String pattern = String.format("gondola-db-%s.*\\.db", hostId);
                                Arrays.asList(new File("/tmp").listFiles((dir, name) -> name.matches(pattern))).forEach(File::delete);
                                wr.write("SUCCESS: host: " + hostId + " destroyed\n");
                            } else {
                                wr.write("ERROR: Unknown command\n");
                            }
                            break;
                        // block <hostId>
                        case "block":
                            if (split.length == 2) {
                                hostId = split[1];
                                blockGondola();
                            } else {
                                wr.write("ERROR: Unknown command\n");
                            }
                            break;
                        // unblock <hostId>
                        case "unblock":
                            if (split.length == 2) {
                                hostId = split[1];
                                unblockGondola();
                            }
                            break;
                        // processes
                        case "processes":
                            wr.write("SUCCESS: ");
                            processMap.entrySet().stream().forEach(e -> {
                                try {
                                    wr.write(e.getKey() + ":" + (e.getValue().isAlive() ? "alive" : "dead") + " ");
                                } catch (IOException e1) {
                                    logger.error(e1.getMessage(), e1);
                                }
                            });
                            wr.write("\n");
                            break;
                        case "slowdown":
                        case "resume":

                        default:
                            wr.write("ERROR: Unknown command\n");
                    }
                    wr.flush();
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        private void unblockGondola() {
            // TODO: unblock it
        }

        private void blockGondola() {
            // TODO: check os. use iptables.
        }

        private boolean killGondola(String hostId) {
            Process process = processMap.get(hostId);
            if (process != null) {
                process.destroyForcibly();
                return true;
            }
            return false;
        }

        private void execGondola(String hostId, String args) throws IOException {
            args = "./bin/gondola.sh " + args;
            logger.info("Launching gondola: {}", args);

            ProcessBuilder pb = new ProcessBuilder(args.split("\\s"));
            Map<String, String> env = pb.environment();
            env.put("JAVACMD", System.getProperty("java.home") + "/bin/java");
            env.put("JAVA_OPTS",
                String.format("-ea -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=%d", getJdwpPort(hostId)));

            // Redirect output to a log file
            File outFile = new File(String.format("logs/gondola-%s.log", hostId));
            pb.redirectOutput(outFile);
            pb.redirectError(outFile);

            // Start process
            Process process = pb.start();
            processMap.put(hostId, process);
        }

        /*
         * Returns the port for the specified hostId
         */
        private int getJdwpPort(String hostId) {
            Integer port = jdwpPortMap.get(hostId);
            if (port == null) {
                port = jdwpPort.getAndIncrement();
                jdwpPortMap.put(hostId, port);
            }
            return port;
        }
    }
}
