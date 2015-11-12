/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;

public class CliClient {
    static Logger logger = LoggerFactory.getLogger(CliClient.class);

    String hostname;
    int port;
    Channel channel;

    public CliClient(String hostname, int port, int socketTimeout) throws Exception {
        this.hostname = hostname;
        this.port = port;
        channel = new Channel(hostname, port, socketTimeout);

        try {
            // If the connection succeeds, there must have been a left over process. Kill it.
            channel.connect();
            logger.info("Gondola command instance {}:{} is alive. Killing it...", hostname, port);
            quit();
        } catch (Exception e) {
            // Could not connect, which means there wasn't a process to kill
        }
    }

    public boolean isOperational() {
        try {
            return channel.send("s", false) != null;
        } catch (Exception e) {
            return false;
        }
    }
    
    public int commit(String command) throws Exception {
        String[] results = channel.send("c " + command).split(" ");
        return Integer.parseInt(results[1]);
    }

    public String getMode() throws Exception {
        return channel.send("m");
    }

    /**
     * @param timeout -1 means no timeout. In milliseconds.
     */
    public String forceLeader(int timeout) throws Exception {
        return channel.send("F " + timeout);
    }

    public String getLastIndex() throws Exception {
        return channel.send("l");
    }

    /**
     * @param timeout Timeout is in milliseconds. -1 indicate no timeout.
     */
    public String getCommand(int index, int timeout) throws Exception {
        return channel.send(String.format("g %d %d", index, timeout));
    }

    public String getStatus() throws Exception {
        return channel.send("s");
    }

    public boolean isLeader() throws Exception {
        return getMode().equals("LEADER");
    }

    public String enableNastyStorage(boolean on) throws Exception {
        return channel.send("n " + (on ? "on" : "off"));
    }

    public void quit() throws Exception {
        try {
            channel.send("q", false);
        } catch (EOFException e) {
            // This is expected
        }
    }

    public int getPort() {
        return port;
    }
}
