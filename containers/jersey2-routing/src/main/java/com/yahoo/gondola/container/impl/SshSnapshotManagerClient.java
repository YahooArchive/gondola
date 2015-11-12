/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.container.impl;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.IdentityRepository;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.OpenSSHConfig;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.agentproxy.AgentProxyException;
import com.jcraft.jsch.agentproxy.Connector;
import com.jcraft.jsch.agentproxy.ConnectorFactory;
import com.jcraft.jsch.agentproxy.RemoteIdentityRepository;
import com.yahoo.gondola.container.client.SnapshotManagerClient;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * The SnapshotManager client implementation using SSH (JSCH)
 */
public class SshSnapshotManagerClient implements SnapshotManagerClient {

    JSch ssh;

    /**
     * Constructor.
     *
     * @param hostname
     * @throws JSchException
     * @throws IOException
     * @throws AgentProxyException
     */
    public SshSnapshotManagerClient(String hostname) throws JSchException, IOException, AgentProxyException {
        OpenSSHConfig config = OpenSSHConfig.parse("~/.ssh/config");
        ssh = new JSch();
        ssh.setConfigRepository(config);
        ssh.setKnownHosts("~/.ssh/known_hosts");

        ConnectorFactory cf = ConnectorFactory.getDefault();
        Connector con = cf.createConnector();
        IdentityRepository irepo = new RemoteIdentityRepository(con);
        ssh.setIdentityRepository(irepo);

        Session session = ssh.getSession(hostname);
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect();
        Channel channel = session.openChannel("exec");
        ((ChannelExec) channel).setCommand("ls /");
        channel.setInputStream(null);
        InputStream in = channel.getInputStream();
        channel.connect();
        byte[] tmp = new byte[1024];
        while (true) {
            while (in.available() > 0) {
                int i = in.read(tmp, 0, 1024);
                if (i < 0) {
                    break;
                }
                System.out.print(new String(tmp, 0, i));
            }
            if (channel.isClosed()) {
                if (in.available() > 0) {
                    continue;
                }
                System.out.println("exit-status: " + channel.getExitStatus());
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (Exception ee) {
                // ignored
            }
        }
        channel.disconnect();
        session.disconnect();
    }

    @Override
    public URI startSnapshot(String siteId) {
        return null;
    }

    @Override
    public void stopSnapshot(URI snapshotUri) {

    }

    @Override
    public SnapshotStatus getSnapshotStatus(URI snapshotUri) {
        return null;
    }

    @Override
    public void restoreSnapshot(String siteId, URI snapshotUri) {

    }
}
