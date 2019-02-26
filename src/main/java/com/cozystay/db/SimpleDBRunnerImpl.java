package com.cozystay.db;

import com.cozystay.model.SyncOperation;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.text.ParseException;
import java.util.*;

public class SimpleDBRunnerImpl implements DBRunner {

    static Map<String, DataSource> dataSourceMap = new HashMap<>();
    private static Logger logger = LoggerFactory.getLogger(SimpleDBRunnerImpl.class);

    private final String address;
    private final int port;
    private final String user;
    private final String password;
    private final boolean silent;

    public SimpleDBRunnerImpl(Properties prop, String prefix, boolean silent) throws ParseException {
        this.silent = silent;

        String dbAddress;
        if ((dbAddress = prop.getProperty(prefix + ".dbAddress")) == null) {
            throw new ParseException(prefix + ".dbAddress", 1);
        }
        String dbUser;
        if ((dbUser = prop.getProperty(prefix + ".dbUser")) == null) {
            throw new ParseException(prefix + ".dbUser", 2);
        }
        String dbPassword;
        if ((dbPassword = prop.getProperty(prefix + ".dbPassword")) == null) {
            throw new ParseException(prefix + ".dbPassword", 3);
        }
        Integer dbPort;
        if ((dbPort = Integer.valueOf(prop.getProperty(prefix + ".dbPort"))) <= 0) {
            throw new ParseException(prefix + ".dbPort", 4);
        }
        this.address = dbAddress;
        this.port = dbPort;
        this.user = dbUser;
        this.password = dbPassword;
    }

    public SimpleDBRunnerImpl(String address, int port, String user, String password) {

        this.address = address;
        this.port = port;
        this.user = user;
        this.password = password;
        this.silent = false;
    }

    @Override
    public int write(SyncOperation operation) throws SQLException {
        Connection conn = null;
        Statement statement = null;
        try {
            conn = getConnection(
                    String.format("jdbc:mysql://%s:%d/%s?verifyServerCertificate=false&useSSL=true&autoReconnect=true&serverTimezone=UTC+8",
                            this.address,
                            this.port,
                            operation.getTask().getDatabase()),
                    this.user,
                    this.password);

            statement = conn.createStatement();
            String sql = operation.buildSql();

            logger.info("executing sql {}", sql);
            if (sql == null) {
                return 0;
            }
            statement.addBatch("SET FOREIGN_KEY_CHECKS = 0");
            statement.addBatch(sql);
            statement.addBatch("SET FOREIGN_KEY_CHECKS = 1");
            return statement.executeBatch()[1];


        } catch (SQLException e) {
            logger.error(e.getMessage());
            if (e instanceof CommunicationsException) {
                return -1;
            }
            throw e;
        } finally {
            closeStatement(conn, statement);
        }


    }

    @Override
    public List<Map<String, Object>> query(String dbName, String queryString) throws SQLException {
        Connection conn = null;
        Statement statement = null;
        try {
            conn = getConnection(
                    String.format("jdbc:mysql://%s:%d/%s?verifyServerCertificate=false&useSSL=true&autoReconnect=true&serverTimezone=Asia/Shanghai",
                            this.address,
                            this.port,
                            dbName),
                    this.user,
                    this.password);

            statement = conn.createStatement();

            if (queryString == null) {
                return null;
            }
            logger.info("executing sql {}", queryString);
            System.out.format("[Info] Running query %s in %s\n", queryString, address);
            ResultSet set = statement.executeQuery(queryString);
            ResultSetMetaData metaData = set.getMetaData();
            List<Map<String, Object>> mapList = new LinkedList<>();
            while (set.next()) {
                Map<String, Object> objectMap = new HashMap<>();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    objectMap.put(metaData.getColumnName(i), getData(metaData, set, i));
                }

                mapList.add(objectMap);
            }
            return mapList;

        } catch (SQLException e) {
            logger.error(e.getMessage());
            if (e instanceof CommunicationsException) {
                return null;
            }
            throw e;
        } finally {
            closeStatement(conn, statement);
        }

    }

    @Override
    public boolean update(String dbName, String queryString) {
        Connection conn = null;
        Statement statement = null;
        if (queryString == null) {
            return false;
        }

        try {
            conn = getConnection(
                    String.format("jdbc:mysql://%s:%d/%s?verifyServerCertificate=false&useSSL=true&autoReconnect=true&serverTimezone=UTC",
                            this.address,
                            this.port,
                            dbName),
                    this.user,
                    this.password);

            statement = conn.createStatement();

            logger.info("executing sql {}", queryString);
            statement.addBatch("SET FOREIGN_KEY_CHECKS = 0");
            statement.addBatch(queryString);
            statement.addBatch("SET FOREIGN_KEY_CHECKS = 1");
            return statement.executeBatch()[1] != 0;


        } catch (SQLException e) {
            logger.error(e.getMessage());
            return false;
        } finally {
            closeStatement(conn, statement);
        }

    }

    @Override
    public String getDBInfo() {
        return String.format("%s:%d", this.address, this.port);
    }

    private Object getData(ResultSetMetaData metaData, ResultSet set, int i) {
        try {
            int dataType = metaData.getColumnType(i);
            switch (dataType) {
                case Types.BIGINT:
                case Types.INTEGER:
                case Types.SMALLINT:
                case Types.TINYINT:
                    return set.getInt(i);
                case Types.FLOAT:
                case Types.DOUBLE:
                case Types.DECIMAL:
                    return set.getFloat(i);
                case Types.VARCHAR:
                case Types.NCHAR:
                case Types.CHAR:
                case Types.LONGVARCHAR:
                    return set.getString(i);
//                    return StringEscapeUtils.escapeJava(set.getString(i));
                case Types.DATE:
                    return set.getDate(i);
                case Types.TIME:
                    return set.getTime(i);
                case Types.TIMESTAMP:
                    return set.getTimestamp(i);
                case Types.BOOLEAN:
                case Types.BIT:
                    return set.getBoolean(i);
                case Types.ARRAY:
                case Types.BLOB:
                case Types.BINARY:
                    return set.getBytes(i);
                default:
                    return null;
            }
        } catch (SQLException e) {
            return null;
        }
    }

    private void closeStatement(Connection conn, Statement statement) {
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
            dataSourceMap.put(url, dataSource);

        }
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return null;
        }
    }
}
