package com.cozystay.db;

import com.cozystay.model.SyncOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SimpleDBWriterImpl implements Writer {

    private static Logger logger = LoggerFactory.getLogger(SimpleDBWriterImpl.class);

    private final String address;
    private final int port;
    private final String user;
    private final String password;

    public SimpleDBWriterImpl(String address, int port, String user, String password) {

        this.address = address;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    @Override
    public boolean write(SyncOperation operation) {
        Connection conn = null;
        Statement statement = null;
        try {
            conn = DriverManager.getConnection(
                    String.format("jdbc:mysql://%s:%d/%s?verifyServerCertificate=false&useSSL=true",
                            this.address,
                            this.port,
                            operation.getTask().getDatabase()),
                    this.user,
                    this.password);

            statement = conn.createStatement();
            String sql = operation.buildSql();
            logger.info("executing sql {}", sql);
            return sql != null && statement.executeUpdate(sql) > 0;

        } catch (SQLException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            return false;
        } finally {
            try {
                if (statement != null && !statement.isClosed()) {
                    statement.close();
                }
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


    }
}
