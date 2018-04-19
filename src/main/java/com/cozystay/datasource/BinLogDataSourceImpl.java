package com.cozystay.datasource;

import com.aliyun.drc.clusterclient.message.ClusterMessage;
import com.cozystay.db.SimpleDBWriterImpl;
import com.cozystay.db.Writer;
import com.cozystay.model.SchemaRuleCollection;
import com.cozystay.model.SyncOperation;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.util.*;

public abstract class BinLogDataSourceImpl implements DataSource {
    private final SchemaRuleCollection schemaRuleCollection;
    private final Writer writer;
    private final BinaryLogClient client;
    private String serverId;
    private String subscribeInstanceID;
    private final String dbAddress;
    private final Integer dbPort;
    private final String dbUser;
    private final String dbPassword;
    private Map<String, SchemaDatabase> databaseMap;

    protected BinLogDataSourceImpl(Properties prop, String prefix) throws Exception {
        String dbAddress, accessKey, accessSecret, subscribeInstanceID, dbUser, dbPassword;
        int dbPort;
        if ((dbAddress = prop.getProperty(prefix + ".dbAddress")) == null) {
            throw new ParseException(prefix + ".dbAddress", 1);
        }
        if ((dbUser = prop.getProperty(prefix + ".dbUser")) == null) {
            throw new ParseException(prefix + ".dbUser", 2);
        }
        if ((dbPassword = prop.getProperty(prefix + ".dbPassword")) == null) {
            throw new ParseException(prefix + ".dbPassword", 3);
        }
        if ((dbPort = Integer.valueOf(prop.getProperty(prefix + ".dbPort"))) <= 0) {
            throw new ParseException(prefix + ".dbPort", 4);
        }

        if ((subscribeInstanceID = prop.getProperty(prefix + ".subscribeInstanceID")) == null) {
            throw new ParseException(prefix + ".subscribeInstanceID", 7);
        }


        this.schemaRuleCollection = SchemaRuleCollection.loadRules(prop);
        this.dbAddress = dbAddress;
        this.dbPort = dbPort;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;

        System.out.printf("Start BinLogDataSource using config: %s:%d, instance %s %n",
                dbAddress,
                dbPort,
                subscribeInstanceID);
        writer = new SimpleDBWriterImpl(dbAddress, dbPort, dbUser, dbPassword);

        client = new BinaryLogClient(dbAddress, dbPort, dbUser, dbPassword);
        databaseMap = new HashMap<>();

    }

    @Override
    public String getName() {
        return this.subscribeInstanceID;
    }

    @Override
    public void writeDB(SyncOperation operation) {
        this.writer.write(operation);
    }

    @Override
    public void start() {
        loadDB();
        EventDeserializer eventDeserializer = new EventDeserializer();
//        eventDeserializer.setEventDataDeserializer(EventType.GTID,new NullEventDataDeserializer());
//        eventDeserializer.setEventDataDeserializer(EventType.ROTATE,new NullEventDataDeserializer());
        client.registerEventListener(new BinaryLogClient.EventListener() {
            @Override
            public void onEvent(Event event) {

                switch (event.getHeader().getEventType()) {
                    case TABLE_MAP: {
                        TableMapEventData data = event.getData();
                        System.out.printf("%s, %s %n",
                                data.getDatabase(),
                                data.getTable());
                        for (int type : data.getColumnMetadata()) {
                            System.out.print(type + " ");
                        }


                        break;

                    }
                    case UPDATE_ROWS: {
                        UpdateRowsEventData data = event.getData();
                        System.out.println(data.toString());
                        break;
                    }

                    case DELETE_ROWS: {
                        DeleteRowsEventData data = event.getData();
                        System.out.println(data.toString());
                        break;
                    }
                    case WRITE_ROWS: {
                        WriteRowsEventData data = event.getData();
                        System.out.println(data.toString());
                        break;
                    }
                    default:


                }


            }


        });

        client.registerLifecycleListener(new BinaryLogClient.LifecycleListener() {
            @Override
            public void onConnect(BinaryLogClient binaryLogClient) {
                Long binlogPos = binaryLogClient.getBinlogPosition();
//                binaryLogClient.getBinlogFilename();

                System.out.printf("bin log position %s.%d%n", binaryLogClient.getBinlogFilename(), binlogPos);
                // read schema from db


            }

            @Override
            public void onCommunicationFailure(BinaryLogClient binaryLogClient, Exception e) {

            }

            @Override
            public void onEventDeserializationFailure(BinaryLogClient binaryLogClient, Exception e) {

            }

            @Override
            public void onDisconnect(BinaryLogClient binaryLogClient) {

            }
        });
        try {
            client.setEventDeserializer(eventDeserializer);
            client.connect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void loadDB() {
        String connString = String.format("jdbc:mysql://%s:%d/?user=%s&password=%s",
                this.dbAddress,
                this.dbPort,
                this.dbUser,
                this.dbPassword);
        Connection connection;
        try {
            connection = DriverManager.getConnection(connString);
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet tableResultSet = metaData.getTables(null, "public", "%", new String[]{"TABLE"})) {
                while (tableResultSet.next()) {
                    String dbName = tableResultSet.getString(1);
                    SchemaDatabase database;
                    if (databaseMap.containsKey(dbName)) {
                        database = databaseMap.get(dbName);
                    } else {
                        database = new SchemaDatabase(dbName, "UTF-8");//TODO: find out encoding
                        databaseMap.put(dbName, database);
                    }
                    String tableName = tableResultSet.getString(3);
                    SchemaTable table = new SchemaTable(tableName, "UTF-8");
                    database.addTable(table);
                    try (ResultSet columnResultSet = metaData.getColumns(null, "public", tableName, "%")) {
                        int index = 1;
                        while (columnResultSet.next()) {
                            String columnName = columnResultSet.getString("COLUMN_NAME");
                            String columnType = columnResultSet.getString("DATA_TYPE");
                            Integer typeVal = Integer.valueOf(columnType);
//                            String typeName = columnResultSet.getString("TYPE_NAME");
                            table.addField(new SchemaField(columnName, typeVal, index));
                            index++;
//                            System.out.printf("%s %s %s%n", columnName, columnType, typeName);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Override
    public boolean shouldFilterMessage(ClusterMessage message) {
        return false;
    }

    @Override
    public void stop() {
        try {
            client.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    class SchemaDatabase {
        final String dbName;
        final String dbEncoding;
        final List<SchemaTable> tableList;

        SchemaDatabase(String dbName, String dbEncoding) {
            this.dbName = dbName;
            this.dbEncoding = dbEncoding;
            tableList = new ArrayList<>();

        }

        void addTable(SchemaTable table) {
            this.tableList.add(table);
        }


    }

    class SchemaField {
        final String columnName;
        final ColumnType columnType;
        final int index;

        SchemaField(String columnName, int columnType, int index) {
            this.columnName = columnName;
            this.columnType = ColumnType.valueOf(columnType);
            this.index = index;
        }
    }

    enum ColumnType {
        BIT(-7),
        UNSIGNED_BIGINT(-5),
        TEXT(-1),
        CHAR(1),
        INT(4),
        DOUBLE(8),
        TIMESTAMP(93),
        VARCHAR(12);
        private int value;


        ColumnType(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.name();
        }

        public Integer getValue() {
            return this.value;
        }

        static ColumnType valueOf(int value) {
            switch (value) {
                case -7:
                    return BIT;
                case -5:
                    return UNSIGNED_BIGINT;
                case -1:
                    return TEXT;
                case 1:
                    return CHAR;
                case 4:
                    return INT;
                case 8:
                    return DOUBLE;
                case 12:
                    return VARCHAR;
                case 93:
                    return TIMESTAMP;
                default:
                    return null;

            }
        }

    }

    ;

    class SchemaTable {
        final String tableName;
        final String tableEncoding;
        final List<SchemaField> fieldList;

        SchemaTable(String tableName, String tableEncoding) {
            this.tableName = tableName;

            this.tableEncoding = tableEncoding;
            this.fieldList = new ArrayList<>();
        }

        void addField(SchemaField field) {
            this.fieldList.add(field);
        }

        public List<SchemaField> getFieldList() {
            return fieldList;
        }

        public String getTableEncoding() {
            return tableEncoding;
        }

        public String getTableName() {
            return tableName;
        }
    }


}
