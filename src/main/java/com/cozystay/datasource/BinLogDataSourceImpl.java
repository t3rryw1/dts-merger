package com.cozystay.datasource;

import com.aliyun.drc.clusterclient.message.ClusterMessage;
import com.cozystay.db.SimpleDBWriterImpl;
import com.cozystay.db.Writer;
import com.cozystay.model.SchemaRuleCollection;
import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;
import com.cozystay.model.SyncTaskBuilder;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
import java.text.ParseException;
import java.util.*;
import java.util.Date;

public abstract class BinLogDataSourceImpl implements DataSource {
    private final SchemaRuleCollection schemaRuleCollection;
    private final Writer writer;
    private final BinaryLogClient client;

    private String subscribeInstanceID;
    private final String dbAddress;
    private final Integer dbPort;
    private final String dbUser;
    private final String dbPassword;
    private Map<String, SchemaDatabase> databaseMap;

    protected BinLogDataSourceImpl(Properties prop, String prefix) throws Exception {
        String dbAddress, subscribeInstanceID, dbUser, dbPassword;
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

        this.subscribeInstanceID = subscribeInstanceID;
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
        loadDBSchema(this.schemaRuleCollection);
        EventDeserializer eventDeserializer = new EventDeserializer();
        client.registerEventListener(new BinaryLogClient.EventListener() {
            private String currentTable;
            private String currentDB;

            @Override
            public void onEvent(Event event) {


                switch (event.getHeader().getEventType()) {
                    case TABLE_MAP: {
                        TableMapEventData data = event.getData();
                        System.out.printf("%s, %s %n",
                                data.getDatabase(),
                                data.getTable());
                        currentTable = data.getTable();
                        currentDB = data.getDatabase();
                        break;

                    }
                    case UPDATE_ROWS:
                    case DELETE_ROWS:
                    case WRITE_ROWS: {
                        SyncTaskBuilder builder = SyncTaskBuilder.getInstance();
                        builder.setSource(subscribeInstanceID);
                        builder.setTableName(currentTable);
                        builder.setDatabase(currentDB);
                        builder.setOperationTime(new Date(event.getHeader().getTimestamp()));
                        UuidBuilder uuidBuilder = new UuidBuilder();
                        SchemaTable table = databaseMap.get(currentDB).getTable(currentTable);

                        switch (event.getHeader().getEventType()) {
                            case UPDATE_ROWS: {
                                builder.setOperationType(SyncOperation.OperationType.UPDATE);
                                UpdateRowsEventData data = event.getData();
                                BitSet updated = data.getIncludedColumns();
                                BitSet beforeUpdate = data.getIncludedColumnsBeforeUpdate();
                                Map.Entry<Serializable[], Serializable[]> values = data.getRows().get(0);
                                for (int i = 0; i < table.getFieldList().size(); i++) {
                                    SchemaField field = table.getFieldList().get(i);
                                    if (!updated.get(i) || !beforeUpdate.get(i)) {
                                        continue;
                                    }
                                    Serializable oldValue = values.getKey()[i];
                                    Serializable newValue = values.getValue()[i];
                                    if (oldValue != null) {
                                        System.out.printf("field name:%s , type: %s , actual value type %s%n:",
                                                field.columnName,
                                                field.columnType.name(),
                                                oldValue.getClass().toString());
                                    } else {
                                        System.out.printf("field name:%s , type: %s , actual value null%n:",
                                                field.columnName,
                                                field.columnType.name());

                                    }
                                    if (field.isPrimary) {
                                        uuidBuilder.addValue(oldValue.toString());
                                    }

                                    SyncOperation.SyncItem item = buildItem(field, oldValue, newValue);
                                    if (item == null){
                                        continue;
                                    }
                                    if(item.isIndex){
                                        builder.addItem(item);
                                    }
                                    if(item.hasChange()) {
                                        builder.addItem(item);
                                    }
                                }
                            }
                            break;
                            case DELETE_ROWS: {
                                builder.setOperationType(SyncOperation.OperationType.DELETE);
                                DeleteRowsEventData deleteData = event.getData();
                                BitSet updated = deleteData.getIncludedColumns();
                                deleteData.getRows();
                                break;
                            }
                            case WRITE_ROWS: {
                                builder.setOperationType(SyncOperation.OperationType.CREATE);
                                WriteRowsEventData writeData = event.getData();
                                BitSet updated = writeData.getIncludedColumns();
                                writeData.getRows();
                                break;
                            }
                        }
                        builder.setUuid(uuidBuilder.build());
                        SyncTask task = builder.build();
//                        System.out.println(data.toString());

                        if (task != null) {
                            consumeData(task);
                        }


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

    private SyncOperation.SyncItem buildItem(SchemaField field, Serializable oldValue, Serializable newValue) {

        switch (field.columnType) {
            case CHAR:
                return new SyncOperation.SyncItem<>(field.columnName,
                        oldValue.toString(),
                        newValue.toString(),
                        field.columnType,
                        field.isPrimary);
        }

        return new SyncOperation.SyncItem<>(field.columnName,
                oldValue,
                newValue,
                field.columnType,
                field.isPrimary);
    }

    private void loadDBSchema(SchemaRuleCollection collection) {
        String connString = String.format("jdbc:mysql://%s:%d/",
                this.dbAddress,
                this.dbPort);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(connString, dbUser, dbPassword);
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
                    database.addTable(tableName, table);

                    List<String> indexFields = tableIndexFields(metaData,
                            collection,
                            dbName,
                            tableName);

                    try (ResultSet columnResultSet = metaData.getColumns(null, "public", tableName, "%")) {
                        int index = 1;
                        while (columnResultSet.next()) {
                            String columnName = columnResultSet.getString("COLUMN_NAME");
                            String columnType = columnResultSet.getString("DATA_TYPE");
                            String typeName = columnResultSet.getString("TYPE_NAME");
                            System.out.printf("%s %s %s%n", columnName, columnType, typeName);
                            if (indexFields.contains(columnName)) {
                                table.addField(new SchemaField(columnName, typeName, index, true));
                            } else {
                                table.addField(new SchemaField(columnName, typeName, index, false));
                            }
                            index++;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    private List<String> tableIndexFields(DatabaseMetaData metaData,
                                          SchemaRuleCollection ruleCollection,
                                          String dbName,
                                          String tableName) {

        List<String> presetFields = ruleCollection.getPrimaryKeys(dbName, tableName);
        if (presetFields != null) {
            return presetFields;
        }
        try {
            List<String> fieldsFromSchema = new ArrayList<>();
            ResultSet set = metaData.getPrimaryKeys(dbName, null, tableName);
            while (set.next()) {
                fieldsFromSchema.add(set.getString(4));
            }
            return fieldsFromSchema;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
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
        final Map<String, SchemaTable> tableList;

        SchemaDatabase(String dbName, String dbEncoding) {
            this.dbName = dbName;
            this.dbEncoding = dbEncoding;
            tableList = new HashMap<>();

        }

        void addTable(String tableName, SchemaTable table) {
            this.tableList.put(tableName, table);
        }

        SchemaTable getTable(String tableName) {
            return this.tableList.get(tableName);
        }

    }

    class SchemaField {
        final String columnName;
        final SyncOperation.SyncItem.ColumnType columnType;
        final int index;
        final boolean isPrimary;

        SchemaField(String columnName, String columnType, int index, boolean isPrimary) {
            this.columnName = columnName;
            this.columnType = SyncOperation.SyncItem.ColumnType.fromString(columnType);
            this.index = index;
            this.isPrimary = isPrimary;
        }
    }


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
