/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.impl;

import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.LogEntry;
import com.yahoo.gondola.Storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

// TODO: Cache prepared statements for performance.
// TODO: Once noticed that db was missing many tail entries after start up. Should add some kind of check for this.
public class H2dbStorage implements Storage {
    Logger logger = LoggerFactory.getLogger(H2dbStorage.class);

    Queue<LogEntry> pool = new ConcurrentLinkedQueue<>();
    Connection c;

    // Config variables
    int maxCommandSize;

    public H2dbStorage(Gondola gondola, String hostId) throws Exception {
        // Get configs
        maxCommandSize = gondola.getConfig().getInt("raft.command_max_size");
        String user = gondola.getConfig().get("storage_h2.user");
        String password = gondola.getConfig().get("storage_h2.password");
        String url = gondola.getConfig().get("storage_h2.url");
        url = url.replace("$hostId", hostId);

        logger.info("Initializing H2DB storage. maxCommandSize={} url={}", maxCommandSize, url);

        Class.forName("org.h2.Driver");
        c = DriverManager.getConnection(url, user, password);
        c.setAutoCommit(true);
        logger.info("H2DB autoCommit={}", c.getAutoCommit());

        Statement statement = c.createStatement();
        //statement.execute("SET DATABASE SQL SYNTAX MYS TRUE");

        // TODO: change varchar to binary.
        statement.execute("CREATE TABLE IF NOT EXISTS logs ("
                          + "memberId INTEGER NOT NULL,"
                          + "term INTEGER NOT NULL,"
                          + "index INTEGER NOT NULL,"
                          //+ "command BINARY(1500),"
                          + "command VARCHAR(5000),"
                          + "PRIMARY KEY (memberId,index)"
                          + ")");

        statement.execute("CREATE TABLE IF NOT EXISTS MemberInfo ("
                          + "memberId INTEGER NOT NULL,"
                          + "term INTEGER DEFAULT 1,"
                          + "votedFor INTEGER DEFAULT -1,"
                          + "maxGap INTEGER DEFAULT 0,"
                          + "address VARCHAR(256) DEFAULT NULL,"
                          + "pid VARCHAR(256) DEFAULT NULL,"
                          + "PRIMARY KEY (memberId)"
                          + ")");
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    // TODO: this is slow
    @Override
    public boolean isOperational() {
        try {
            return c.isValid(1);
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public String getAddress(int memberId) throws Exception {
        String sql = "SELECT address FROM MemberInfo WHERE memberId=" + memberId;
        Statement statement = c.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        if (resultSet.next()) {
            return resultSet.getString("address");
        }
        return null;
    }

    @Override
    public void setAddress(int memberId, String address) throws Exception {
        String sql = "MERGE INTO MemberInfo(memberId,address) KEY(memberId) VALUES(?,?)";
        PreparedStatement preparedStatement = c.prepareStatement(sql);
        preparedStatement.setInt(1, memberId);
        preparedStatement.setString(2, address);
        int i = preparedStatement.executeUpdate();
        if (i != 1) {
            throw new SQLException("setAddress() failed");
        }
    }

    @Override
    public void saveVote(int memberId, int currentTerm, int votedFor) throws Exception {
        String sql = "MERGE INTO MemberInfo(memberId,term,votedFor) KEY(memberId) VALUES(?,?,?)";
        PreparedStatement preparedStatement = c.prepareStatement(sql);
        preparedStatement.setInt(1, memberId);
        preparedStatement.setInt(2, currentTerm);
        preparedStatement.setInt(3, votedFor);
        int i = preparedStatement.executeUpdate();
        if (i < 1) {
            throw new SQLException(String.format("save(memberId=%d, currentTerm=%d, votedFor=%d) failed",
                                                 memberId, currentTerm, votedFor));
        }
    }

    @Override
    public boolean hasLogEntry(int memberId, int term, int index) throws Exception {
        String sql = "SELECT 1 FROM logs WHERE memberId=? AND term=? AND index=?";
        PreparedStatement preparedStatement = c.prepareStatement(sql);
        preparedStatement.setInt(1, memberId);
        preparedStatement.setInt(2, term);
        preparedStatement.setInt(3, index);
        ResultSet resultSet = preparedStatement.executeQuery();
        return resultSet.next();
    }

    @Override
    public int getCurrentTerm(int memberId) throws Exception {
        String sql = "SELECT term FROM MemberInfo WHERE memberId=" + memberId;
        Statement statement = c.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        if (resultSet.next()) {
            return resultSet.getInt("term");
        }
        return 1;
    }

    @Override
    public int getVotedFor(int memberId) throws Exception {
        String sql = "SELECT votedFor FROM MemberInfo WHERE memberId=" + memberId;
        Statement statement = c.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        if (resultSet.next()) {
            return resultSet.getInt("votedFor");
        }
        return -1;
    }

    @Override
    public int getMaxGap(int memberId) throws Exception {
        String sql = "SELECT maxGap FROM MemberInfo WHERE memberId=" + memberId;
        Statement statement = c.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        if (resultSet.next()) {
            return resultSet.getInt("maxGap");
        }
        return 0;
    }

    @Override
    public void setMaxGap(int memberId, int maxGap) throws Exception {
        String sql = "MERGE INTO MemberInfo(memberId,maxGap) KEY(memberId) VALUES(?,?)";
        PreparedStatement preparedStatement = c.prepareStatement(sql);
        preparedStatement.setInt(1, memberId);
        preparedStatement.setInt(2, maxGap);
        int i = preparedStatement.executeUpdate();
        if (i != 1) {
            throw new SQLException("setMaxGap() failed");
        }
    }

    @Override
    public String getPid(int memberId) throws Exception {
        String sql = "SELECT pid FROM MemberInfo WHERE memberId=" + memberId;
        Statement statement = c.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        if (resultSet.next()) {
            return resultSet.getString("pid");
        }
        return null;
    }

    @Override
    public void setPid(int memberId, String pid) throws Exception {
        String sql = "MERGE INTO MemberInfo(memberId,pid) KEY(memberId) VALUES(?,?)";
        PreparedStatement preparedStatement = c.prepareStatement(sql);
        preparedStatement.setInt(1, memberId);
        preparedStatement.setString(2, pid);
        int i = preparedStatement.executeUpdate();
        if (i != 1) {
            throw new SQLException("setPid() failed");
        }
    }

    @Override
    public int count(int memberId) throws Exception {
        String sql = "SELECT count(*) FROM logs WHERE memberId=" + memberId;
        Statement statement = c.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        if (resultSet.next()) {
            return resultSet.getInt(1);
        }
        return 0;
    }

    @Override
    public LogEntry getLogEntry(int memberId, int index) throws Exception {
        LogEntry logEntry = checkout();
        ResultSet resultSet = getLogEntryResult(memberId, index);
        if (resultSet.next()) {
            logEntry.memberId = memberId;
            logEntry.term = resultSet.getInt("term");
            logEntry.index = index;

            // Read the binary data
            /*
            byte[] bytes = resultSet.getBytes("command");
            System.arraycopy(bytes, 0, logEntry.buffer, 0, bytes.length);
            logEntry.size = bytes.length;
            */
            String command = resultSet.getString("command");
            byte[] bytes = command.getBytes();
            System.arraycopy(bytes, 0, logEntry.buffer, 0, bytes.length);
            logEntry.size = bytes.length;
            return logEntry;
        }
        return null;
    }

    private ResultSet getLogEntryResult(int memberId, int index) throws SQLException {
        String sql = "SELECT term, command FROM logs WHERE memberId=? AND index=?";
        PreparedStatement preparedStatement = c.prepareStatement(sql);
        preparedStatement.setInt(1, memberId);
        preparedStatement.setInt(2, index);
        return preparedStatement.executeQuery();
    }

    @Override
    public LogEntry getLastLogEntry(int memberId) throws Exception {
        String sql = "SELECT max(index) FROM logs where memberId=?";
        PreparedStatement preparedStatement = c.prepareStatement(sql);
        preparedStatement.setInt(1, memberId);
        ResultSet resultSet = preparedStatement.executeQuery();

        int lastIndex = 0;
        if (resultSet.next()) {
            lastIndex = resultSet.getInt(1);
        }
        return getLogEntry(memberId, lastIndex);
    }

    @Override
    public void appendLogEntry(int memberId, int term, int index, byte[] buffer, int bufferOffset, int bufferLen)
        throws Exception {
        String sql = "INSERT INTO logs (memberId, term, index, command) VALUES(?, ?, ?, ?)";

        PreparedStatement preparedStatement = c.prepareStatement(sql);
        preparedStatement.setInt(1, memberId);
        preparedStatement.setInt(2, term);
        preparedStatement.setInt(3, index);
        preparedStatement.setString(4, new String(Arrays.copyOfRange(buffer, bufferOffset, bufferOffset + bufferLen)));
        //preparedStatement.setBinaryStream(4, new ByteArrayInputStream(buffer, bufferOffset, bufferLen));
        int i = preparedStatement.executeUpdate();
        if (i != 1) {
            throw new SQLException(String.format("Failed to insert memberId=%d, index=%d, size=%d. Return=%d",
                                                 memberId, index, bufferLen, i));
        }
    }

    @Override
    public void delete(int memberId, int index) throws Exception {
        String sql = "DELETE FROM logs WHERE memberId=? AND index=?";
        PreparedStatement preparedStatement = c.prepareStatement(sql);
        preparedStatement.setInt(1, memberId);
        preparedStatement.setInt(2, index);
        preparedStatement.execute();
    }

    @Override
    public void checkin(LogEntry entry) {
        pool.add(entry);
    }

    LogEntry checkout() {
        LogEntry le = pool.poll();
        if (le == null) {
            le = new LogEntry(this, maxCommandSize);
        }
        return le;
    }
}
