package com.cozystay.datasource;

import com.aliyun.drc.clusterclient.message.ClusterMessage;
import com.cozystay.db.SimpleDBWriterImpl;
import com.cozystay.db.Writer;
import com.cozystay.db.schema.SchemaField;
import com.cozystay.db.schema.SchemaLoader;
import com.cozystay.db.schema.SchemaRuleCollection;
import com.cozystay.db.schema.SchemaTable;
import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;
import com.cozystay.model.SyncTaskBuilder;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import org.apache.commons.lang3.SerializationUtils;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.*;

public abstract class BinLogDataSourceImpl implements DataSource {
    private final SchemaRuleCollection schemaRuleCollection;
    private final Writer writer;
    private final BinaryLogClient client;

    private String subscribeInstanceID;
    private SchemaLoader schemaLoader;

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
        this.schemaLoader = new SchemaLoader(dbAddress, dbPort, dbUser, dbPassword);


        System.out.printf("Start BinLogDataSource using config: %s:%d, instance %s %n",
                dbAddress,
                dbPort,
                subscribeInstanceID);
        writer = new SimpleDBWriterImpl(dbAddress, dbPort, dbUser, dbPassword);

        client = new BinaryLogClient(dbAddress, dbPort, dbUser, dbPassword);

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
        schemaLoader.loadDBSchema(schemaRuleCollection);
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
                        SchemaTable table = schemaLoader.getTable(currentDB, currentTable);

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
//                                    if (oldValue != null) {
//                                        System.out.printf("field name:%s , type: %s , actual value type %s%n:",
//                                                field.columnName,
//                                                field.columnType.name(),
//                                                oldValue.getClass().getName());
//                                    } else {
//                                        System.out.printf("field name:%s , type: %s , actual value null%n:",
//                                                field.columnName,
//                                                field.columnType.name());
//
//                                    }
                                    if (field.isPrimary) {
                                        assert oldValue != null;
                                        uuidBuilder.addValue(oldValue.toString());
                                    }

                                    SyncOperation.SyncItem item = buildItem(field,
                                            oldValue,
                                            newValue,
                                            SyncOperation.OperationType.UPDATE);
                                    if (item.isIndex) {
                                        builder.addItem(item);
                                    }
                                    if (item.hasChange()) {
                                        builder.addItem(item);
                                    }
                                }
                            }
                            break;
                            case DELETE_ROWS: {
                                builder.setOperationType(SyncOperation.OperationType.DELETE);
                                DeleteRowsEventData deleteData = event.getData();
                                BitSet updated = deleteData.getIncludedColumns();
                                Serializable[] values = deleteData.getRows().get(0);
                                for (int i = 0; i < table.getFieldList().size(); i++) {
                                    SchemaField field = table.getFieldList().get(i);
                                    if (!updated.get(i)) {
                                        continue;
                                    }
                                    Serializable value = values[i];
//                                    if (value != null) {
//                                        System.out.printf("field name:%s , type: %s , actual value type %s%n:",
//                                                field.columnName,
//                                                field.columnType.name(),
//                                                value.getClass().getName());
//                                    } else {
//                                        System.out.printf("field name:%s , type: %s , actual value null%n:",
//                                                field.columnName,
//                                                field.columnType.name());
//
//                                    }
                                    if (field.isPrimary) {
                                        assert value != null;

                                        uuidBuilder.addValue(value.toString());
                                    }

                                    SyncOperation.SyncItem item = buildItem(field,
                                            value,
                                            null,
                                            SyncOperation.OperationType.DELETE);
                                    if (item.isIndex) {
                                        builder.addItem(item);
                                    }
                                    if (item.hasChange()) {
                                        builder.addItem(item);
                                    }
                                }
                                break;
                            }
                            case WRITE_ROWS: {
                                builder.setOperationType(SyncOperation.OperationType.CREATE);
                                WriteRowsEventData writeData = event.getData();
                                BitSet created = writeData.getIncludedColumns();
                                Serializable[] values = writeData.getRows().get(0);
                                for (int i = 0; i < table.getFieldList().size(); i++) {
                                    SchemaField field = table.getFieldList().get(i);
                                    if (!created.get(i)) {
                                        continue;
                                    }
                                    Serializable value = values[i];
//                                    if (value != null) {
//                                        System.out.printf("field name:%s , type: %s , actual value type %s%n:",
//                                                field.columnName,
//                                                field.columnType.name(),
//                                                value.getClass().getName());
//                                    } else {
//                                        System.out.printf("field name:%s , type: %s , actual value null%n:",
//                                                field.columnName,
//                                                field.columnType.name());
//
//                                    }
                                    if (field.isPrimary) {
                                        assert value != null;
                                        uuidBuilder.addValue(value.toString());
                                    }

                                    SyncOperation.SyncItem item = buildItem(field,
                                            null,
                                            value,
                                            SyncOperation.OperationType.CREATE);
                                    if (item.isIndex) {
                                        builder.addItem(item);
                                    }
                                    if (item.hasChange()) {
                                        builder.addItem(item);
                                    }
                                }
                                break;
                            }
                        }
                        builder.setUuid(uuidBuilder.build());
                        SyncTask task = builder.build();
                        if (task == null) {
                            break;
                        }
                        if (schemaRuleCollection.filter(task.getOperations().get(0))) {
                            break;
                        }
                        consumeData(task);

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
                //todo: binlog location logics

                System.out.printf("bin log position %s.%d%n", binaryLogClient.getBinlogFilename(), binlogPos);


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


    private String transByteArrToStr(Serializable value) {
        byte[] bytes = SerializationUtils.serialize(value);
        return new String(bytes);
    }

    private Boolean checkTypes(String value) {
        List<String> allowedTypes = new ArrayList<>(Arrays.asList(
                "java.lang.String",
                "java.lang.Byte",
                "java.lang.Short",
                "java.lang.Integer",
                "java.lang.Long",
                "java.lang.Float",
                "java.lang.Double",
                "java.lang.BigDecimal",
                "java.lang.Character",
                "java.lang.Boolean"
        ));
        return allowedTypes.contains(value);
    }

    private SyncOperation.SyncItem buildItem(SchemaField field,
                                             Serializable oldValue,
                                             Serializable newValue,
                                             SyncOperation.OperationType operationType) {

        if(checkTypes(oldValue.getClass().getName()) && checkTypes(newValue.getClass().getName())){
            return new SyncOperation.SyncItem<>(field.columnName,
                    oldValue,
                    newValue,
                    field.columnType,
                    field.isPrimary);
        }

        switch (field.columnType) {
            case BIT:
            case BOOL:
            case BOOLEAN:{
                return new SyncOperation.SyncItem<>(field.columnName,
                        oldValue,
                        newValue,
                        field.columnType,
                        field.isPrimary);
            }

            case ENUM:
                return new SyncOperation.SyncItem<>(field.columnName,
                        oldValue,
                        newValue,
                        field.columnType,
                        field.isPrimary);

            case DOUBLE:
            case UNSIGNED_DOUBLE:
            case DECIMAL:
            case UNSIGNED_DECIMAL: {
                return new SyncOperation.SyncItem<>(field.columnName,
                        oldValue,
                        newValue,
                        field.columnType,
                        field.isPrimary);
            }

            case TINYTEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case TEXT: {
                if (newValue.getClass().getName().equals("[B")) {
                    newValue = transByteArrToStr(newValue);
                }
                if (oldValue.getClass().getName().equals("[B")) {
                    oldValue = transByteArrToStr(oldValue);
                }
                return new SyncOperation.SyncItem<>(field.columnName,
                        oldValue,
                        newValue,
                        field.columnType,
                        field.isPrimary);
            }
            case INT:
            case UNSIGNED_INT:
            case TINYINT:
            case UNSIGNED_TINYINT:
            case SMALLINT:
            case UNSIGNED_SMALLINT:
            case MEDIUMINT:
            case UNSIGNED_MEDIUMINT:
            case BIGINT:
            case UNSIGNED_BIGINT: {
                System.out.print(oldValue.getClass().getName()+"\n");
                System.out.print(newValue.getClass().getName()+"\n");
                return new SyncOperation.SyncItem<>(field.columnName,
                        oldValue,
                        newValue,
                        field.columnType,
                        field.isPrimary);
            }
            case DATE:
            case TIMESTAMP:
            case DATETIME:
            case TIME:
            case YEAR: {
                System.out.print(oldValue.getClass().getName()+"\n");
                System.out.print(newValue.getClass().getName()+"\n");
                return new SyncOperation.SyncItem<>(field.columnName,
                        oldValue,
                        newValue,
                        field.columnType,
                        field.isPrimary);
            }
            case JSON:
            case VARCHAR:
            case CHAR: {
                System.out.print(oldValue.getClass().getName()+"\n");
                System.out.print(newValue.getClass().getName()+"\n");
                return new SyncOperation.SyncItem<>(field.columnName,
                        oldValue.toString(),
                        newValue.toString(),
                        field.columnType,
                        field.isPrimary);

            }
            default:{
                throw new IllegalArgumentException("Can't parse Illegal ColumnType");
            }
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


}
