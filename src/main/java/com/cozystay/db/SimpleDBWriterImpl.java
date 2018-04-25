package com.cozystay.db;

import com.cozystay.model.SyncOperation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SimpleDBWriterImpl implements Writer {
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
    public void write(SyncOperation operation) {
        Connection conn = null;
        Statement statement = null;
        try {
            conn = DriverManager.getConnection(
                    String.format("jdbc:mysql://%s:%d/%s?user=%s&password=%s",
                            this.address,
                            this.port,
                            operation.getTask().getDatabase(),
                            this.user,
                            this.password));

            statement = conn.createStatement();
    //        statement.execute(operation.buildSql());

        } catch (SQLException e) {
            e.printStackTrace();
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
