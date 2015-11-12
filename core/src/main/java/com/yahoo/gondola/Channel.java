/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * A channel is an abstraction that provides communication between the local member and a remote member.
 * Communication is done via reading and writing to streams.
 * The channel can be operational or not. When not operational, the channel will automatically try
 * to reestablish connectivity to the remote member until it is operational again.
 *
 * Usage:
 *   InputStream in = null;
 *   while (true) {
 *     try {
 *       in = channel.getInputStream(in, false);
 *       in.read();
 *     } catch (Exception e) {
 *       in = channel.getInputStream(in, true);
 *     }
 *   }
 */
public interface Channel extends Stoppable {
    void start() throws Exception;

    void stop();

    /**
     * Returns the id of the remote member for this channel.
     */
    public int getRemoteMemberId();
    
    /**
     * Returns a human-readable string that represents the physical location of the remote member.
     * This is used in dashboards to identify the remote member.
     *
     * @return a non-null string representing the remote member.
     */
    public String getRemoteAddress();
    
    /**
     * Returns true if the input and output streams are valid.
     */
    public boolean isOperational();

    /**
     * Blocks until the channel is operational.
     */
    public void awaitOperational() throws InterruptedException;

    /**
     * Returns an input stream which is used to receive data from the remote member.
     * Blocks until a valid input stream is available.
     *
     * @param in the previously used input stream or null if none.
     * @return a non-null input stream.
     */
    public InputStream getInputStream(InputStream in, boolean errorOccurred) throws InterruptedException;

    /**
     * Returns an output stream which is used to send data to the remote member.
     * Blocks until a valid output stream is available.
     * 
     * @param out the previously used output stream or null if none.
     * @return a non-null output stream.
     */
    public OutputStream getOutputStream(OutputStream out, boolean errorOccurred) throws InterruptedException;
}
