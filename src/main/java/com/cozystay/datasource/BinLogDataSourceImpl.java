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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.BitSet;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public abstract class BinLogDataSourceImpl implements DataSource {
    private final SchemaRuleCollection schemaRuleCollection;
    private final Writer writer;
    private final BinaryLogClient client;

    private String subscribeInstanceID;
    private SchemaLoader schemaLoader;
    private Jedis redisClient;

    protected BinLogDataSourceImpl(Properties prop, String prefix) throws Exception {
        String dbAddress,
                subscribeInstanceID,
                dbUser,
                dbPassword,
                redisHost,
                redisPassword;
        int dbPort, redisPort;
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
            throw new ParseException(prefix + ".subscribeInstanceID", 5);
        }

        if ((redisHost = prop.getProperty("redis.host")) == null) {
            throw new ParseException("redis.host", 6);
        }

        if ((redisPort = Integer.valueOf(prop.getProperty("redis.port"))) <= 0) {
            throw new ParseException("redis.port", 7);
        }
        if ((redisPassword = prop.getProperty("redis.password")) == null) {
            throw new ParseException("redis.password", 8);
        }


        this.subscribeInstanceID = subscribeInstanceID;
        this.schemaRuleCollection = SchemaRuleCollection.loadRules(prop);
        this.schemaLoader = new SchemaLoader(dbAddress, dbPort, dbUser, dbPassword);
        JedisPool jedisPool = new JedisPool(redisHost, redisPort);

        redisClient = jedisPool.getResource();
        if(!redisPassword.equals("")){
            redisClient.auth(redisPassword);

        }

        System.out.printf("Start BinLogDataSource using config: %s:%d, instance %s %n",
                dbAddress,
                dbPort,
                subscribeInstanceID);
        writer = new SimpleDBWriterImpl(dbAddress, dbPort, dbUser, dbPassword);

        client = new BinaryLogClient(dbAddress, dbPort, dbUser, dbPassword);
        SyncTaskBuilder.addSource(subscribeInstanceID);

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
                        try {
                            SyncTaskBuilder builder = SyncTaskBuilder.getInstance();
                            builder.setSource(subscribeInstanceID);
                            builder.setTableName(currentTable);
                            builder.setDatabase(currentDB);
                            builder.setOperationTime(new Date(event.getHeader().getTimestamp()));
                            UuidBuilder uuidBuilder = new UuidBuilder();
                            SchemaTable table = schemaLoader.getTable(currentDB, currentTable);
                            if (table == null) {
                                break;
                            }

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
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }

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

                System.out.printf("bin log position on connect %s.%d%n", binaryLogClient.getBinlogFilename(), binlogPos);
                redisClient.set("binlogFile-" + subscribeInstanceID, binaryLogClient.getBinlogFilename());
                redisClient.set("binlogPosition-" + subscribeInstanceID, String.valueOf(binlogPos));


            }

            @Override
            public void onCommunicationFailure(BinaryLogClient binaryLogClient, Exception e) {

            }

            @Override
            public void onEventDeserializationFailure(BinaryLogClient binaryLogClient, Exception e) {

            }

            @Override
            public void onDisconnect(BinaryLogClient binaryLogClient) {
                Long binlogPos = binaryLogClient.getBinlogPosition();

                System.out.printf("bin log position on disconnect %s.%d%n", binaryLogClient.getBinlogFilename(), binlogPos);
                redisClient.set("binlogFile-" + subscribeInstanceID, binaryLogClient.getBinlogFilename());
                redisClient.set("binlogPosition-" + subscribeInstanceID, String.valueOf(binlogPos));

            }
        });
        try {
            client.setEventDeserializer(eventDeserializer);
            client.setConnectTimeout(10000);
            client.setKeepAliveConnectTimeout(10000);
            String binlogFileName, binlogPosition;
            if (
                    (binlogFileName = redisClient.get("binlogFile-" + this.subscribeInstanceID))
                            !=
                            null
                            &&
                            (binlogPosition = redisClient.get("binlogPosition-" + this.subscribeInstanceID))
                                    !=
                                    null) {
                client.setBinlogFilename(binlogFileName);
                client.setBinlogPosition(Long.valueOf(binlogPosition));
            }
            client.connect(10000);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }


    private String transByteArrToStr(byte[] value) throws UnsupportedEncodingException {
        return new String(value, "UTF-8");
    }

    private enum AllowType {
        String("java.lang.String"),
        Byte("java.lang.Byte"),
        Short("java.lang.Short"),
        Integer("java.lang.Integer"),
        Long("java.lang.Long"),
        Float("java.lang.Float"),
        Double("java.lang.Double"),
        BigDecimal("java.math.BigDecimal"),
        Character("java.lang.Character"),
        SqlDate("java.sql.Date"),
        UtilDate("java.util.Date"),
        TimeStamp("java.sql.Timestamp"),
        Boolean("java.lang.Boolean");

        private final String fullName;

        AllowType(String type) {
            fullName = type;
        }
    }

    private Boolean checkTypes(Serializable value, AllowType dataType) {
        if (value == null) return true;
        return value.getClass().getName().equals(dataType.fullName);
    }

    private SyncOperation.SyncItem buildItem(SchemaField field,
                                             Serializable oldValue,
                                             Serializable newValue,
                                             SyncOperation.OperationType operationType) throws UnsupportedEncodingException {

        switch (field.columnType) {
            case BIT:
            case BOOL:
            case BOOLEAN: {
                if (checkTypes(oldValue, AllowType.Integer) && checkTypes(newValue, AllowType.Integer)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            }

            case ENUM:
                if (checkTypes(oldValue, AllowType.Integer) && checkTypes(newValue, AllowType.Integer)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            case DOUBLE:
            case UNSIGNED_DOUBLE: {
                if (checkTypes(oldValue, AllowType.Double) && checkTypes(newValue, AllowType.Double)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            }
            case DECIMAL:
            case UNSIGNED_DECIMAL: {
                if (checkTypes(oldValue, AllowType.BigDecimal) && checkTypes(newValue, AllowType.BigDecimal)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            }
            case JSON:
            case TINYTEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case TEXT: {
                if (checkTypes(oldValue, AllowType.String) && checkTypes(newValue, AllowType.String)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                } else {
                    if (newValue.getClass().getName().equals("[B")) {
                        newValue = transByteArrToStr((byte[]) newValue);
                    } else {
                        break;
                    }
                    if (oldValue.getClass().getName().equals("[B")) {
                        oldValue = transByteArrToStr((byte[]) oldValue);
                    } else {
                        break;
                    }
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
            }
            case INT:
            case UNSIGNED_INT:
            case TINYINT:
            case UNSIGNED_TINYINT:
            case SMALLINT:
            case UNSIGNED_SMALLINT:
            case MEDIUMINT:
            case UNSIGNED_MEDIUMINT: {
                if (checkTypes(oldValue, AllowType.Integer) && checkTypes(newValue, AllowType.Integer)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            }
            case BIGINT:
            case UNSIGNED_BIGINT: {
                if (checkTypes(oldValue, AllowType.Long) && checkTypes(newValue, AllowType.Long)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            }
            case DATE: {
                if (checkTypes(oldValue, AllowType.SqlDate) && checkTypes(newValue, AllowType.SqlDate)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            }
            case DATETIME: {
                if (checkTypes(oldValue, AllowType.UtilDate) && checkTypes(newValue, AllowType.UtilDate)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            }
            case TIME:
            case YEAR: {
                if (checkTypes(oldValue, AllowType.SqlDate) && checkTypes(newValue, AllowType.SqlDate)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            }
            case TIMESTAMP: {
                if (checkTypes(oldValue, AllowType.TimeStamp) && checkTypes(newValue, AllowType.TimeStamp)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            }
            case VARCHAR:
            case CHAR: {
                if (checkTypes(oldValue, AllowType.String) && checkTypes(newValue, AllowType.String)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            }
            default: {
                throw new IllegalArgumentException("Can't parse Illegal ColumnType");
            }
        }
        throw new IllegalArgumentException("Illegal value type to actual field");
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
