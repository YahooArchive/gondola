/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.impl;

import com.yahoo.gondola.Gondola;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

/**
 */
public class NastyChannel extends SocketChannel {
    final static Logger logger = LoggerFactory.getLogger(NastyChannel.class);

    OutputStream lastOutputStream;
    OutputStream nastyOutputStream;
    boolean enabled;

    public NastyChannel(Gondola gondola, int memberId, int toMemberId) {
        super(gondola, memberId, toMemberId);
    }

    /**
     * See Channel.getOutputStream().
     *
     * @return a non-null output stream
     */
    @Override
    public OutputStream getOutputStream(OutputStream out, boolean errorOccurred) throws InterruptedException {
        OutputStream os = super.getOutputStream(out, errorOccurred);
        if (os != lastOutputStream) {
            lastOutputStream = os;
            nastyOutputStream = new NastyOutputStream(lastOutputStream);
        }
        return nastyOutputStream;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void enable(boolean on) {
        enabled = on;
    }

    /*********************** non-public methods ********************/

    class NastyOutputStream extends OutputStream {
        OutputStream wrapped;
        byte[] savedB;
        int savedOff;
        int savedLen;
        
        NastyOutputStream(OutputStream wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public void write(byte[] b) throws IOException {
            throw new IllegalStateException("not implemented");
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            // Random delay
            if (enabled && Math.random() < .0001) {
                try {
                    Thread.sleep((int) (Math.random() * 50));
                } catch (InterruptedException e) {
                    logger.error(e.getMessage(), e);
                }
            }

            // Capture a random message for resending later
            if (enabled && (savedB == null || Math.random() < .0001)) {
                savedB = b.clone();
                savedOff = off;
                savedLen = len;
                logger.info("Nasty channel capturing message");
            }

            if (enabled && Math.random() < .0001) {
                // Randomly resend an old message
                logger.info("Nasty channel resending old message");
                wrapped.write(savedB, savedOff, savedLen);
            } else {
                wrapped.write(b, off, len);
            }
        }

        @Override
        public void write(int b) throws IOException {
            throw new IllegalStateException("not implemented");
        }

        @Override
        public void close() throws IOException {
            wrapped.close();
        }
    }
}
