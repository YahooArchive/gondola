/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.tsunami;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AgentClient {
    static Logger logger = LoggerFactory.getLogger(AgentClient.class);

    final String hostId;
    final String hostname;
    final int agentPort;
    final Channel channel;
    final CliClient gondolaCc;

    // Tracks whether the gondola server is up or down
    boolean up = false;
    
    public AgentClient(String hostId, String hostname, int agentPort, CliClient gondolaCc) throws Exception {
        this.hostId = hostId;
        this.hostname = hostname;
        this.agentPort = agentPort;
        this.gondolaCc = gondolaCc;
        logger.info("Connecting to gondola agent at {}:{}", hostname, agentPort);
        channel = new Channel(hostname, agentPort, 0);
    }

    public void createInstance() {
        try {
            channel.send(String.format("create %s -hostid %s -clusterid cluster1 -port %d -config %s start",
                                       hostId, hostId, gondolaCc.port, "conf/gondola-tsunami.conf"));
            up = true;
        } catch (Exception e) {
            logger.error("Create {} failed: {}", hostId, e.getMessage());
        }
    }

    public void destroyInstance() {
        try {
            // Try killing the process directly first
            gondolaCc.quit();

            // Now ask the agent to kill the process
            channel.send(String.format("destroy %s", hostId));
            up = false;
        } catch (Exception e) {
            logger.error("Destroy {} failed : {}", hostId, e.getMessage());
        }
    }

    public String fullDestroyInstance() throws Exception {
        return channel.send(String.format("full_destroy %s", hostId));
    }

    public String blockInstance(String fromHostId, String toHostId) {
        // TODO
        return null;
    }

    public String unblockInstance(String fromHostId, String toHostId) {
        // TODO
        return null;
    }

    public String slowDown(String fromHostId, String toHostId) {
        // TODO
        return null;
    }

    public String resumeSlowDown(String fromHostId, String toHostId) {
        // TODO
        return null;
    }

    public String slowDownAll(String hostId) {
        // TODO
        return null;
    }

    public ChannelResult resumeSlowDownAll(String hostId) {
        // TODO
        return null;
    }
}
