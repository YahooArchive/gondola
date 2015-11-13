/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.impl;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.LogEntry;
import com.yahoo.gondola.Storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This is a hack of a storage implementation used to test performance.
 * It cannot be used to test HA.
 * It only stores the currentTerm, votedFor, lastTerm, and lastIndex values.
 * Everything else is just made up.
 */
public class FakeStorage implements Storage {
    Logger logger = LoggerFactory.getLogger(FakeStorage.class);
    Config config;

    Queue<LogEntry> pool = new ConcurrentLinkedQueue<>();
    Map<Integer, Member> members = new ConcurrentHashMap<>();

    File file;
    RandomAccessFile raFile;
    int avgCommandSize = 8;

    // Config variables
    int maxCommandSize;

    public FakeStorage(Gondola gondola, String hostId) throws Exception {
        logger.info("Initialize FakeStorage");
        this.config = gondola.getConfig();
        config.registerForUpdates(config1 -> {
            maxCommandSize = config1.getInt("raft.command_max_size");
        });

        // TODO: remove the hardcode. Need to figure out how to add properties to config, like JDBC.
        file = new File("/tmp/gondola-" + hostId + ".dat");

        // Read saved information
        if (file.exists()) {
            try {
                raFile = new RandomAccessFile(file, "rws");
                while (raFile.getFilePointer() < raFile.length()) {
                    String line = raFile.readUTF();
                    logger.info("FakeStorage {}: {}", file, line);
                    String[] parts = line.split(",");
                    Member m = new Member(Integer.parseInt(parts[0]),
                            Integer.parseInt(parts[1]),
                            Integer.parseInt(parts[2]),
                            Integer.parseInt(parts[3]),
                            Integer.parseInt(parts[4]),
                            Integer.parseInt(parts[5]));
                    members.put(m.id, m);
                }
            } catch (Exception e) {
                logger.warn("Ignoring error", e);
            }
        } else {
            raFile = new RandomAccessFile(file, "rws");
        }
        new Writer().start();
    }


    @Override
    public boolean isOperational() {
        return true;
    }

    @Override
    public String getAddress(int memberId) throws Exception {
        return getMember(memberId).address;
    }

    @Override
    public void setAddress(int memberId, String address) throws Exception {
        Member member = getMember(memberId);
        member.address = address;
    }

    @Override
    public void saveVote(int memberId, int currentTerm, int votedFor) throws Exception {
        Member member = getMember(memberId);
        member.currentTerm = currentTerm;
        member.votedFor = votedFor;
    }

    @Override
    public int getCurrentTerm(int memberId) throws Exception {
        return getMember(memberId).currentTerm;
    }

    @Override
    public int getVotedFor(int memberId) throws Exception {
        return getMember(memberId).votedFor;
    }

    @Override
    public int getMaxGap(int memberId) throws Exception {
        return getMember(memberId).maxGap;
    }

    @Override
    public void setMaxGap(int memberId, int maxGap) throws Exception {
        getMember(memberId).maxGap = maxGap;
    }

    @Override
    public String getPid(int memberId) throws Exception {
        return getMember(memberId).pid;
    }

    @Override
    public void setPid(int memberId, String pid) throws Exception {
        getMember(memberId).pid = pid;
    }

    @Override
    public int count(int memberId) throws Exception {
        return getMember(memberId).lastIndex;
    }

    @Override
    public void appendLogEntry(int memberId, int term, int index,
                               byte[] buffer, int bufferOffset, int bufferLen) throws Exception {
        if (avgCommandSize == 0) {
            avgCommandSize = bufferLen;
        } else {
            avgCommandSize = (avgCommandSize + bufferLen) / 2;
        }
        Member m = getMember(memberId);
        m.lastTerm = term;
        m.lastIndex = Math.max(m.lastIndex, index);
    }

    @Override
    public void delete(int memberId, int index) throws Exception {
        Member m = getMember(memberId);
        m.lastIndex = Math.min(m.lastIndex, index - 1);
    }

    @Override
    public boolean hasLogEntry(int memberId, int term, int index) throws Exception {
        Member member = getMember(memberId);
        return index <= member.lastIndex;
    }

    @Override
    public LogEntry getLogEntry(int memberId, int index) throws Exception {
        Member m = getMember(memberId);
        if (index > m.lastIndex) {
            return null;
        }
        LogEntry le = checkout();
        le.term = 1;
        le.index = index;
        le.size = avgCommandSize;
        return le;
    }

    @Override
    public LogEntry getLastLogEntry(int memberId) throws Exception {
        Member m = getMember(memberId);
        LogEntry le = checkout();
        le.term = 1;
        le.index = m.lastIndex;
        le.size = avgCommandSize;
        return le;
    }

    @Override
    public void checkin(LogEntry entry) {
        pool.add(entry);
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    LogEntry checkout() {
        LogEntry le = pool.poll();
        if (le == null) {
            le = new LogEntry(this, maxCommandSize);
        }
        return le;
    }

    class Member {
        int id = -1;
        int currentTerm = 1;
        int votedFor = -1;
        int maxGap = 0;
        int lastTerm = 0;
        int lastIndex = 0;
        String address = null;
        String pid = null;

        Member(int id) {
            this.id = id;
        }

        Member(int id, int currentTerm, int votedFor, int maxGap, int lastTerm, int lastIndex) {
            this.id = id;
            this.currentTerm = currentTerm;
            this.votedFor = votedFor;
            this.maxGap = maxGap;
            this.lastTerm = lastTerm;
            this.lastIndex = lastIndex;
        }

        public String toString() {
            return String.format("%d,%d,%d,%d,%d,%d", id, currentTerm, votedFor, maxGap, lastTerm, lastIndex);
        }
    }

    Member getMember(int memberId) throws Exception {
        Member member = members.get(memberId);
        if (member == null) {
            member = new Member(memberId);
            members.putIfAbsent(memberId, member);
        }
        return member;
    }

    class Writer extends Thread {
        public void run() {
            while (true) {
                int i = 0;
                try {
                    raFile.seek(0);
                    for (Map.Entry<Integer, Member> e : members.entrySet()) {
                        Member m = e.getValue();
                        raFile.writeUTF(m.toString());
                    }
                    Thread.sleep(100);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }
}
