package com.cozystay.db;

import com.cozystay.model.SyncOperation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

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
        try {
            conn = DriverManager.getConnection(
                    String.format("jdbc:mysql://%s:%d/%s?user=%s&password=%s",
                            this.address,
                            this.port,
                            operation.getTask().getDatabase(),
                            this.user,
                            this.password));

            //TODO: sql to write data

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


    }
}
