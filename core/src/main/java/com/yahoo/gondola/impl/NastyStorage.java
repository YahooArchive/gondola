/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.impl;

import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.LogEntry;
import com.yahoo.gondola.Storage;
import com.yahoo.gondola.GondolaException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Nasty storage.
 */
public class NastyStorage implements Storage {

    Logger logger = LoggerFactory.getLogger(NastyStorage.class);

    final Storage storage;

    boolean tracing;
    boolean enabled;

    public NastyStorage(Gondola gondola, String hostId) throws GondolaException {
        String storageClassName = gondola.getConfig().get(gondola.getConfig().get("storage.nasty.impl") + ".class");
        try {
            storage = (Storage) Class.forName(storageClassName).getConstructor(Gondola.class, String.class)
                    .newInstance(gondola, hostId);
        } catch (Exception e) {
            throw new GondolaException(e);
        }
        tracing = gondola.getConfig().getBoolean("storage.nasty.tracing");
    }

    @Override
    public void start() {
        storage.start();
    }

    @Override
    public boolean stop() {
        return storage.stop();
    }

    @Override
    public boolean isOperational() {
        return storage.isOperational();
    }

    @Override
    public String getAddress(int memberId) throws GondolaException {
        return storage.getAddress(memberId);
    }

    @Override
    public void setAddress(int memberId, String address) throws GondolaException {
        storage.setAddress(memberId, address);
    }

    double random(int index) throws GondolaException {
        double r = Math.random();
        if (enabled && r < .001) {
            throw new GondolaException(GondolaException.Code.ERROR, "Nasty exception for index=" + index);
        }
        return r;
    }

    @Override
    public void saveVote(int memberId, int currentTerm, int votedFor) throws GondolaException {
        storage.saveVote(memberId, currentTerm, votedFor);
    }

    @Override
    public boolean hasLogEntry(int memberId, int term, int index) throws GondolaException {
        return storage.hasLogEntry(memberId, term, index);
    }

    @Override
    public int getCurrentTerm(int memberId) throws GondolaException {
        return storage.getCurrentTerm(memberId);
    }

    @Override
    public int getVotedFor(int memberId) throws GondolaException {
        return storage.getVotedFor(memberId);
    }

    @Override
    public int getMaxGap(int memberId) throws GondolaException {
        return storage.getMaxGap(memberId);
    }

    @Override
    public void setMaxGap(int memberId, int maxGap) throws GondolaException {
        random(-1);
        storage.setMaxGap(memberId, maxGap);
    }

    @Override
    public String getPid(int memberId) throws GondolaException {
        return storage.getPid(memberId);
    }

    @Override
    public void setPid(int memberId, String pid) throws GondolaException {
        random(-1);
        storage.setPid(memberId, pid);
    }

    @Override
    public int count(int memberId) throws GondolaException {
        return storage.count(memberId);
    }

    @Override
    public LogEntry getLogEntry(int memberId, int index) throws GondolaException {
        random(index);
        return storage.getLogEntry(memberId, index);
    }

    @Override
    public LogEntry getLastLogEntry(int memberId) throws GondolaException {
        random(-1);
        return storage.getLastLogEntry(memberId);
    }

    @Override
    public void appendLogEntry(int memberId, int term, int index, byte[] buffer, int bufferOffset, int bufferLen)
            throws GondolaException, InterruptedException {
        double r = random(index);
        if (enabled && r < .2) {
            int delay = (int) (r * 100);
            if (tracing) {
                logger.info("delaying {} by {} ms", index, delay);
            }
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                throw new GondolaException(e);
            }
        }
        storage.appendLogEntry(memberId, term, index, buffer, bufferOffset, bufferLen);
    }

    @Override
    public void delete(int memberId, int index) throws GondolaException {
        random(index);
        storage.delete(memberId, index);
    }

    @Override
    public void checkin(LogEntry entry) {
        storage.checkin(entry);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void enable(boolean on) {
        enabled = on;
    }
}
