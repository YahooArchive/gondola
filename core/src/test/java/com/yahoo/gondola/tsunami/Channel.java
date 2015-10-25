/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.tsunami;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

/**
 * This class is used to connect to GondolaCommand and GondolaAgent processes.
 */
public class Channel {
    static Logger logger = LoggerFactory.getLogger(Channel.class);

    final String host;
    final int port;
    final int timeout;

    Socket socket;
    BufferedReader rd;
    BufferedWriter wr;

    /**
     * Connect to the specified host:port.
     */
    public Channel(String host, int port, int timeout) throws Exception {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
    }

    public void connect() throws Exception {
        socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), timeout);
        socket.setSoTimeout(timeout);
        rd = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        wr = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    /**
     * Equivalent to send(command, true);
     */
    public String send(String command) throws Exception {
        return send(command, true);
    }

    /**
     * Blocks until the command is successfully sent to the remote process.
     * If the connection to the remote process fails, this method will continuously attempt
     * to reestablish the connection.
     */
    public String send(String command, boolean retry) throws Exception {
        ChannelResult result = null;
        while (true) {
            try {
                // If anything fails, reconnect
                if (socket == null || wr == null) {
                    connect();
                    //logger.info("Connected to {}:{}", host, port);
                }

                wr.write(command + "\n");
                wr.flush();
                String response = rd.readLine();
                if (response == null) {
                    throw new EOFException(command);
                }
                result = new ChannelResult(response);
                break;
            } catch (EOFException e) {
                throw e;
            } catch (Exception e) {
                close();
                if (!retry) {
                    return null;
                }
                try {
                    // Wait a little before trying again, to avoid a spin loop
                    Thread.sleep(1000);
                } catch (Exception e2) {
                    logger.error(e2.getMessage(), e2);
                }
            }
        }
        if (result.success) {
            return result.message;
        }
        throw new Exception(String.format("[%s:%d] %s -> %s", host, port, command, result.message));
    }

    public void close() {
        if (wr != null) {
            try {
                wr.close();
            } catch (IOException e1) {
                logger.error("Channel wr.close: {}", e1.getMessage());
            }
        }
        if (rd != null) {
            try {
                rd.close();
            } catch (IOException e1) {
                logger.error("Channel rd.close: {}", e1.getMessage());
            }
        }
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e1) {
                logger.error("Channel socket.close: {}", e1.getMessage());
            }
            socket = null;
        }
    }
}
