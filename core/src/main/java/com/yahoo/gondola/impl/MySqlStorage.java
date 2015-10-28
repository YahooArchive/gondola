/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.impl;

import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.LogEntry;
import com.yahoo.gondola.Storage;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

// TODO: Cache prepared statements for performance.
public class MySqlStorage implements Storage {
    Logger logger = LoggerFactory.getLogger(MySqlStorage.class);

    Queue<LogEntry> logEntryPool = new ConcurrentLinkedQueue<>();
    HikariDataSource ds;

    // Config variables
    int maxCommandSize;

    public MySqlStorage(Gondola gondola, String hostId) throws Exception {
        // Get configs
        maxCommandSize = gondola.getConfig().getInt("raft.command_max_size");
        String user = gondola.getConfig().get("storage_mysql.user");
        String password = gondola.getConfig().get("storage_mysql.password");
        String url = gondola.getConfig().get("storage_mysql.url");
        url = url.replace("$hostId", hostId);

        logger.info("Initializing MySql storage. maxCommandSize={} url={}", maxCommandSize, url);

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        ds = new HikariDataSource(config);
        try (Connection c = ds.getConnection()) {
            if (!c.getAutoCommit()) {
                throw new IllegalStateException("Auto-commit must be enabled.");
            }
            Statement statement = c.createStatement();

            // TODO: change varchar to binary.
            statement.execute("CREATE TABLE IF NOT EXISTS logs ("
                    + "member_id INT NOT NULL,"
                    + "term INT NOT NULL,"
                    + "indx INT NOT NULL,"
                    + "command VARCHAR(5000),"
                    + "updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                    + "PRIMARY KEY (member_id,indx)"
                    + ")");

            statement.execute("CREATE TABLE IF NOT EXISTS member_info ("
                    + "member_id INT NOT NULL,"
                    + "term INT DEFAULT 1,"
                    + "voted_for INT DEFAULT -1,"
                    + "max_gap INT DEFAULT 0,"
                    + "address VARCHAR(256) DEFAULT NULL,"
                    + "pid VARCHAR(256) DEFAULT NULL,"
                    + "updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                    + "PRIMARY KEY (member_id)"
                    + ")");
        }
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
        try (Connection c = ds.getConnection()) {
            return c.isValid(1);
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public String getAddress(int memberId) throws Exception {
        try (Connection c = ds.getConnection()) {
            String sql = "SELECT address FROM member_info WHERE member_id=?";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getString("address");
            }
            return null;
        }
    }

    @Override
    public void setAddress(int memberId, String address) throws Exception {
        try (Connection c = ds.getConnection()) {
            String sql = "INSERT INTO member_info(member_id,address) VALUES(?,?)"
                    + "ON DUPLICATE KEY UPDATE address=VALUES(address)";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            preparedStatement.setString(2, address);
            int i = preparedStatement.executeUpdate();
            if (i < 1) {
                throw new SQLException("setAddress() failed. Return=" + i);
            }
        }
    }

    @Override
    public void saveVote(int memberId, int currentTerm, int votedFor) throws Exception {
        try (Connection c = ds.getConnection()) {
            String sql = "INSERT INTO member_info(member_id,term,voted_for) VALUES(?,?,?) "
                    + "ON DUPLICATE KEY UPDATE term=VALUES(term), voted_for=VALUES(voted_for)";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            preparedStatement.setInt(2, currentTerm);
            preparedStatement.setInt(3, votedFor);
            int i = preparedStatement.executeUpdate();
            if (i < 1) {
                throw new SQLException(String.format("save(member_id=%d, term=%d, voted_for=%d) failed",
                        memberId, currentTerm, votedFor));
            }
        }
    }

    @Override
    public boolean hasLogEntry(int memberId, int term, int index) throws Exception {
        try (Connection c = ds.getConnection()) {
            String sql = "SELECT 1 FROM logs WHERE member_id=? AND term=? AND indx=?";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            preparedStatement.setInt(2, term);
            preparedStatement.setInt(3, index);
            ResultSet resultSet = preparedStatement.executeQuery();
            return resultSet.next();
        }
    }

    @Override
    public int getCurrentTerm(int memberId) throws Exception {
        try (Connection c = ds.getConnection()) {
            String sql = "SELECT term FROM member_info WHERE member_id=?";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("term");
            }
            return 1;
        }
    }

    @Override
    public int getVotedFor(int memberId) throws Exception {
        try (Connection c = ds.getConnection()) {
            String sql = "SELECT voted_for FROM member_info WHERE member_id=?";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("voted_for");
            }
            return -1;
        }
    }

    @Override
    public int getMaxGap(int memberId) throws Exception {
        try (Connection c = ds.getConnection()) {
            String sql = "SELECT max_gap FROM member_info WHERE member_id=?";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("max_gap");
            }
            return 0;
        }
    }

    @Override
    public void setMaxGap(int memberId, int maxGap) throws Exception {
        try (Connection c = ds.getConnection()) {
            String sql = "INSERT INTO member_info(member_id,max_gap) VALUES(?,?)"
                    + "ON DUPLICATE KEY UPDATE max_gap=VALUES(max_gap)";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            preparedStatement.setInt(2, maxGap);
            int i = preparedStatement.executeUpdate();
            if (i < 1) {
                throw new SQLException("setMaxGap() failed. Return=" + i);
            }
        }
    }

    @Override
    public String getPid(int memberId) throws Exception {
        try (Connection c = ds.getConnection()) {
            String sql = "SELECT pid FROM member_info WHERE member_id=?";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getString("pid");
            }
            return null;
        }
    }

    @Override
    public void setPid(int memberId, String pid) throws Exception {
        try (Connection c = ds.getConnection()) {
            String sql = "INSERT INTO member_info(member_id,pid) VALUES(?,?)"
                    + "ON DUPLICATE KEY UPDATE pid=VALUES(pid)";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            preparedStatement.setString(2, pid);
            int i = preparedStatement.executeUpdate();
            if (i < 1) {
                throw new SQLException("setPid() failed. Return=" + i);
            }
        }
    }

    @Override
    public int count(int memberId) throws Exception {
        try (Connection c = ds.getConnection()) {
            String sql = "SELECT count(*) FROM logs WHERE member_id=?";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
            return 0;
        }
    }

    @Override
    public LogEntry getLogEntry(int memberId, int index) throws Exception {
        try (Connection c = ds.getConnection()) {
            LogEntry logEntry = checkout();
            ResultSet resultSet = getLogEntryResult(c, memberId, index);
            if (resultSet.next()) {
                logEntry.memberId = memberId;
                logEntry.term = resultSet.getInt("term");
                logEntry.index = index;

                // Read the binary data
                byte[] bytes = resultSet.getBytes("command");
                System.arraycopy(bytes, 0, logEntry.buffer, 0, bytes.length);
                logEntry.size = bytes.length;
                return logEntry;
            }
            return null;
        }
    }

    private ResultSet getLogEntryResult(Connection c, int memberId, int index) throws SQLException {
        String sql = "SELECT term, command FROM logs WHERE member_id=? AND indx=?";
        PreparedStatement preparedStatement = c.prepareStatement(sql);
        preparedStatement.setInt(1, memberId);
        preparedStatement.setInt(2, index);
        return preparedStatement.executeQuery();
    }

    @Override
    public LogEntry getLastLogEntry(int memberId) throws Exception {
        try (Connection c = ds.getConnection()) {
            String sql = "SELECT max(indx) FROM logs where member_id=?";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            ResultSet resultSet = preparedStatement.executeQuery();

            int lastIndex = 0;
            if (resultSet.next()) {
                lastIndex = resultSet.getInt(1);
            }
            return getLogEntry(memberId, lastIndex);
        }
    }

    @Override
    public void appendLogEntry(int memberId, int term, int index, byte[] buffer, int bufferOffset, int bufferLen)
            throws Exception {
        try (Connection c = ds.getConnection()) {
            String sql = "INSERT INTO logs(member_id, term, indx, command) VALUES(?, ?, ?, ?)";

            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            preparedStatement.setInt(2, term);
            preparedStatement.setInt(3, index);
            preparedStatement.setBinaryStream(4, new ByteArrayInputStream(buffer, bufferOffset, bufferLen));
            int i = preparedStatement.executeUpdate();
            if (i != 1) {
                throw new SQLException(String.format("Failed to insert member_id=%d, index=%d, size=%d. Return=%d",
                        memberId, index, bufferLen, i));
            }
        }
    }

    @Override
    public void delete(int memberId, int index) throws Exception {
        try (Connection c = ds.getConnection()) {
            String sql = "DELETE FROM logs WHERE member_id=? AND indx=?";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            preparedStatement.setInt(1, memberId);
            preparedStatement.setInt(2, index);
            preparedStatement.execute();
        }
    }

    @Override
    public void checkin(LogEntry entry) {
        logEntryPool.add(entry);
    }

    LogEntry checkout() {
        LogEntry le = logEntryPool.poll();
        if (le == null) {
            le = new LogEntry(this, maxCommandSize);
        }
        return le;
    }
}
