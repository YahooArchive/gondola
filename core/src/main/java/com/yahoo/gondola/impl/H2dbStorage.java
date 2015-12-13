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
import com.yahoo.gondola.GondolaException;

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

/**
 * The type H 2 db storage.
 * TODO: Cache prepared statements for performance.
 * TODO: Once noticed that db was missing many tail entries after start up. Should add some kind of check for this.
 */
public class H2dbStorage implements Storage {
    Logger logger = LoggerFactory.getLogger(H2dbStorage.class);

    Gondola gondola;
    String hostId;
    Queue<LogEntry> pool = new ConcurrentLinkedQueue<>();
    Connection c;

    // Config variables
    int maxCommandSize;

    public H2dbStorage(Gondola gondola, String hostId) throws GondolaException {
        try {
            this.gondola = gondola;
            this.hostId = hostId;

            // Get configs
            maxCommandSize = gondola.getConfig().getInt("raft.command_max_size");

            Class.forName("org.h2.Driver");
            createConnection();
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
            statement.close();
        } catch (ClassNotFoundException | SQLException e) {
            throw new GondolaException(e);
        }
    }

    void createConnection() throws GondolaException {
        Config cfg = gondola.getConfig();
        String user = cfg.get("storage.h2.user");
        String password = cfg.get("storage.h2.password");

        // If there's a store-specific setting, use it; otherwise use default
        String url = cfg.get("storage.h2.url");
        String storeId = cfg.getAttributesForHost(hostId).get("storeId");
        if (storeId != null) {
            String urlKey = "storage." + storeId + ".h2.url";
            if (cfg.has(urlKey)) {
                url = cfg.get(urlKey);
            }
        }
        url = url.replace("$hostId", hostId);

        logger.info("Initializing H2DB storage. maxCommandSize={} url={} user={}", maxCommandSize, url, user);

        try {
            c = DriverManager.getConnection(url, user, password);
            c.setAutoCommit(true);
        } catch (SQLException e) {
            throw new GondolaException(e);
        }
    }

    @Override
    public void start() {
    }

    @Override
    public boolean stop() {
        try {
            c.close();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return true;
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
    public String getAddress(int memberId) throws GondolaException {
        String sql = "SELECT address FROM MemberInfo WHERE memberId=" + memberId;
        try {
            Statement statement = c.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                return resultSet.getString("address");
            }
        } catch (SQLException e) {
            throw new GondolaException(e);
        }
        return null;
    }

    @Override
    public void setAddress(int memberId, String address) throws GondolaException {
        try {
            String sql = "MERGE INTO MemberInfo(memberId,address) KEY(memberId) VALUES(?,?)";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            preparedStatement.setString(2, address);
            int i = preparedStatement.executeUpdate();
            if (i != 1) {
                throw new SQLException("setAddress() failed");
            }
        } catch (SQLException e) {
            throw new GondolaException(e);
        }
    }

    @Override
    public void saveVote(int memberId, int currentTerm, int votedFor) throws GondolaException {
        try {
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
        } catch (SQLException e) {
            throw new GondolaException(e);
        }
    }

    @Override
    public boolean hasLogEntry(int memberId, int term, int index) throws GondolaException {
        try {
            String sql = "SELECT 1 FROM logs WHERE memberId=? AND term=? AND index=?";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            preparedStatement.setInt(2, term);
            preparedStatement.setInt(3, index);
            ResultSet resultSet = preparedStatement.executeQuery();
            return resultSet.next();
        } catch (SQLException e) {
            throw new GondolaException(e);
        }
    }

    @Override
    public int getCurrentTerm(int memberId) throws GondolaException {
        try {
            String sql = "SELECT term FROM MemberInfo WHERE memberId=" + memberId;
            Statement statement = c.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                return resultSet.getInt("term");
            }
        } catch (SQLException e) {
            throw new GondolaException(e);
        }
        return 1;
    }

    @Override
    public int getVotedFor(int memberId) throws GondolaException {
        try {
            String sql = "SELECT votedFor FROM MemberInfo WHERE memberId=" + memberId;
            Statement statement = c.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                return resultSet.getInt("votedFor");
            }
        } catch (SQLException e) {
            throw new GondolaException(e);
        }
        return -1;
    }

    @Override
    public int getMaxGap(int memberId) throws GondolaException {
        try {
            String sql = "SELECT maxGap FROM MemberInfo WHERE memberId=" + memberId;
            Statement statement = c.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                return resultSet.getInt("maxGap");
            }
        } catch (SQLException e) {
            throw new GondolaException(e);
        }
        return 0;
    }

    @Override
    public void setMaxGap(int memberId, int maxGap) throws GondolaException {
        try {
            String sql = "MERGE INTO MemberInfo(memberId,maxGap) KEY(memberId) VALUES(?,?)";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            preparedStatement.setInt(2, maxGap);
            int i = preparedStatement.executeUpdate();
            if (i != 1) {
                throw new SQLException("setMaxGap() failed");
            }
        } catch (SQLException e) {
            throw new GondolaException(e);
        }
    }

    @Override
    public String getPid(int memberId) throws GondolaException {
        try {
            String sql = "SELECT pid FROM MemberInfo WHERE memberId=" + memberId;
            Statement statement = c.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                return resultSet.getString("pid");
            }
        } catch (SQLException e) {
            throw new GondolaException(e);
        }
        return null;
    }

    @Override
    public void setPid(int memberId, String pid) throws GondolaException {
        try {
            String sql = "MERGE INTO MemberInfo(memberId,pid) KEY(memberId) VALUES(?,?)";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            preparedStatement.setString(2, pid);
            int i = preparedStatement.executeUpdate();
            if (i != 1) {
                throw new SQLException("setPid() failed");
            }
        } catch (SQLException e) {
            throw new GondolaException(e);
        }
    }

    @Override
    public int count(int memberId) throws GondolaException {
        try {
            String sql = "SELECT count(*) FROM logs WHERE memberId=" + memberId;
            Statement statement = c.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
        } catch (SQLException e) {
            throw new GondolaException(e);
        }
        return 0;
    }

    @Override
    public LogEntry getLogEntry(int memberId, int index) throws GondolaException {
        try {
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
        } catch (SQLException e) {
            throw new GondolaException(e);
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
    public LogEntry getLastLogEntry(int memberId) throws GondolaException {
        int lastIndex = 0;
        try {
            String sql = "SELECT max(index) FROM logs where memberId=?";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            ResultSet resultSet = preparedStatement.executeQuery();

            lastIndex = 0;
            if (resultSet.next()) {
                lastIndex = resultSet.getInt(1);
            }
        } catch (SQLException e) {
            throw new GondolaException(e);
        }
        return getLogEntry(memberId, lastIndex);
    }

    @Override
    public void appendLogEntry(int memberId, int term, int index, byte[] buffer, int bufferOffset, int bufferLen)
            throws GondolaException {
        try {
            String sql = "INSERT INTO logs (memberId, term, index, command) VALUES(?, ?, ?, ?)";

            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            preparedStatement.setInt(2, term);
            preparedStatement.setInt(3, index);
            preparedStatement.setString(4,
                    new String(Arrays.copyOfRange(buffer, bufferOffset, bufferOffset + bufferLen)));
            //preparedStatement.setBinaryStream(4, new ByteArrayInputStream(buffer, bufferOffset, bufferLen));
            int i = preparedStatement.executeUpdate();
            if (i != 1) {
                throw new SQLException(String.format("Failed to insert memberId=%d, index=%d, size=%d. Return=%d",
                        memberId, index, bufferLen, i));
            }
        } catch (SQLException e) {
            throw new GondolaException(e);
        }
    }

    @Override
    public void delete(int memberId, int index) throws GondolaException {
        try {
            String sql = "DELETE FROM logs WHERE memberId=? AND index=?";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            preparedStatement.setInt(2, index);
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new GondolaException(e);
        }
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
