package com.cozystay.db;

import com.cozystay.model.SyncOperation;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class SimpleDBWriterImpl implements Writer {

    static Map<String, DataSource> dataSourceMap = new HashMap<>();
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
    public boolean write(SyncOperation operation) throws SQLException {
        Connection conn = null;
        Statement statement = null;
        try {
            conn = getConnection(
                    String.format("jdbc:mysql://%s:%d/%s?verifyServerCertificate=false&useSSL=true&autoReconnect=true",
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
            throw e;
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

    private static Connection getConnection(String url, String user, String password) {
        DataSource dataSource;
        if (dataSourceMap.containsKey(url)) {
            dataSource = dataSourceMap.get(url);
        } else {
            ConnectionFactory connectionFactory =
                    new DriverManagerConnectionFactory(url, user, password);

            //
            // Next we'll create the PoolableConnectionFactory, which wraps
            // the "real" Connections created by the ConnectionFactory with
            // the classes that implement the pooling functionality.
            //
            PoolableConnectionFactory poolableConnectionFactory =
                    new PoolableConnectionFactory(connectionFactory, null);

            //
            // Now we'll need a ObjectPool that serves as the
            // actual pool of connections.
            //
            // We'll use a GenericObjectPool instance, although
            // any ObjectPool implementation will suffice.
            //
            ObjectPool<PoolableConnection> connectionPool =
                    new GenericObjectPool<>(poolableConnectionFactory);

            // Set the factory's pool property to the owning pool
            poolableConnectionFactory.setPool(connectionPool);

            //
            // Finally, we create the PoolingDriver itself,
            // passing in the object pool we created.
            //
            dataSource =
                    new PoolingDataSource<>(connectionPool);
            dataSourceMap.put(url,dataSource);

        }
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return null;
        }
    }
}
