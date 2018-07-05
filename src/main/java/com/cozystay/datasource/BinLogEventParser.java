package com.cozystay.datasource;

import com.cozystay.db.schema.SchemaField;
import com.cozystay.db.schema.SchemaLoader;
import com.cozystay.db.schema.SchemaTable;
import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;
import com.cozystay.model.SyncTaskBuilder;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.*;

class BinLogEventParser {
    List<SyncTask> parseTask(Event event,
                             SchemaLoader schemaLoader, String subscribeInstanceID,
                             String currentTable,
                             String currentDB) {
        SchemaTable table = schemaLoader.getTable(currentDB, currentTable);
        List<SyncTask> taskList = new ArrayList<>();

        if (table == null) {
            return taskList;
        }
        try {


            switch (event.getHeader().getEventType()) {
                case UPDATE_ROWS: {
                    UpdateRowsEventData data = event.getData();
                    BitSet updated = data.getIncludedColumns();
                    BitSet beforeUpdate = data.getIncludedColumnsBeforeUpdate();
                    for (int dataIndex = 0; dataIndex < data.getRows().size(); dataIndex++) {
                        SyncTaskBuilder builder = getBuilder(SyncOperation.OperationType.UPDATE,
                                subscribeInstanceID,
                                currentTable,
                                currentDB);
                        UuidBuilder uuidBuilder = new UuidBuilder();

                        Map.Entry<Serializable[], Serializable[]> values = data.getRows().get(dataIndex);
                        for (int i = 0; i < table.getFieldList().size(); i++) {
                            if (!updated.get(i) || !beforeUpdate.get(i)) {
                                continue;
                            }

                            addItemToBuilder(table.getFieldList().get(i),
                                    values.getKey()[i],
                                    values.getValue()[i],
                                    values.getKey()[i],
                                    SyncOperation.OperationType.UPDATE,
                                    builder,
                                    uuidBuilder);
                        }
                        builder.setUuid(uuidBuilder.build());
                        taskList.add(builder.build());

                    }
                    break;

                }
                case DELETE_ROWS: {
                    DeleteRowsEventData deleteData = event.getData();
                    BitSet updated = deleteData.getIncludedColumns();
                    for (int dataIndex = 0; dataIndex < deleteData.getRows().size(); dataIndex++) {
                        SyncTaskBuilder builder = getBuilder(SyncOperation.OperationType.DELETE,
                                subscribeInstanceID,
                                currentTable,
                                currentDB);
                        UuidBuilder uuidBuilder = new UuidBuilder();


                        Serializable[] values = deleteData.getRows().get(dataIndex);
                        for (int i = 0; i < table.getFieldList().size(); i++) {
                            if (!updated.get(i)) {
                                continue;
                            }
                            addItemToBuilder(table.getFieldList().get(i),
                                    values[i],
                                    null,
                                    values[i],
                                    SyncOperation.OperationType.DELETE,
                                    builder,
                                    uuidBuilder);
                        }
                        builder.setUuid(uuidBuilder.build());
                        taskList.add(builder.build());

                    }
                    break;

                }

                case WRITE_ROWS: {
                    WriteRowsEventData writeData = event.getData();
                    BitSet created = writeData.getIncludedColumns();
                    for (int dataIndex = 0; dataIndex < writeData.getRows().size(); dataIndex++) {

                        SyncTaskBuilder builder = getBuilder(SyncOperation.OperationType.CREATE,
                                subscribeInstanceID,
                                currentTable,
                                currentDB);
                        UuidBuilder uuidBuilder = new UuidBuilder();

                        Serializable[] values = writeData.getRows().get(dataIndex);
                        for (int i = 0; i < table.getFieldList().size(); i++) {
                            if (!created.get(i)) {
                                continue;
                            }
                            addItemToBuilder(table.getFieldList().get(i),
                                    null,
                                    values[i],
                                    values[i],
                                    SyncOperation.OperationType.CREATE,
                                    builder,
                                    uuidBuilder);
                        }
                        builder.setUuid(uuidBuilder.build());
                        SyncTask task = builder.build();
                        task.firstOperation().reduceItems();
                        taskList.add(task);

                    }
                    break;

                }
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }

        return taskList;

    }

    private void addItemToBuilder(SchemaField field,
                                  Serializable oldValue,
                                  Serializable newValue,
                                  Serializable primaryValue,
                                  SyncOperation.OperationType type,
                                  SyncTaskBuilder builder,
                                  UuidBuilder uuidBuilder) throws UnsupportedEncodingException {


        if (field.isPrimary) {
            assert primaryValue != null;
            uuidBuilder.addValue(primaryValue.toString());
        }

        SyncOperation.SyncItem item = buildItem(field,
                oldValue,
                newValue,
                type);

        if (item.isIndex) {
            builder.addItem(item);
        } else if (item.hasChange()) {
            builder.addItem(item);
        }
    }

    private SyncTaskBuilder getBuilder(SyncOperation.OperationType type,
                                       String subscribeInstanceID,
                                       String currentTable,
                                       String currentDB) {
        SyncTaskBuilder builder = new SyncTaskBuilder();
        builder.setSource(subscribeInstanceID);
        builder.setTableName(currentTable);
        builder.setDatabase(currentDB);
        builder.setOperationTime(new Date().getTime());
        builder.setOperationType(type);
        return builder;
    }

    private SyncOperation.SyncItem buildItem(SchemaField field,
                                             Serializable oldValue,
                                             Serializable newValue,
                                             SyncOperation.OperationType operationType)
            throws UnsupportedEncodingException {

        switch (Objects.requireNonNull(field.columnType)) {
            case BIT:
            case BOOL:
            case BOOLEAN: {
                if (checkTypes(oldValue, BinLogEventParser.AllowType.Integer)
                        &&
                        checkTypes(newValue, BinLogEventParser.AllowType.Integer)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            }

            case ENUM:
                if (checkTypes(oldValue, BinLogEventParser.AllowType.Integer)
                        &&
                        checkTypes(newValue, BinLogEventParser.AllowType.Integer)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            case DOUBLE:
            case UNSIGNED_DOUBLE: {
                if (checkTypes(oldValue, BinLogEventParser.AllowType.Double)
                        &&
                        checkTypes(newValue, BinLogEventParser.AllowType.Double)) {
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
                if (checkTypes(oldValue, BinLogEventParser.AllowType.BigDecimal)
                        &&
                        checkTypes(newValue, BinLogEventParser.AllowType.BigDecimal)) {
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
                if (checkTypes(oldValue, BinLogEventParser.AllowType.String)
                        &&
                        checkTypes(newValue, BinLogEventParser.AllowType.String)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                } else {
                    if (newValue != null) {
                        if (newValue.getClass().getName().equals("[B")) {
                            newValue = transByteArrToStr((byte[]) newValue);
                        } else {
                            break;
                        }
                    }
                    if (oldValue != null) {
                        if (oldValue.getClass().getName().equals("[B")) {
                            oldValue = transByteArrToStr((byte[]) oldValue);
                        } else {
                            break;
                        }
                    }
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
            }
            case INT:
            case TINYINT:
            case SMALLINT:
            case MEDIUMINT:
                if (checkTypes(oldValue, BinLogEventParser.AllowType.Integer)
                        &&
                        checkTypes(newValue, BinLogEventParser.AllowType.Integer)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            case UNSIGNED_INT:
                if (checkTypes(oldValue, BinLogEventParser.AllowType.Integer)
                        &&
                        checkTypes(newValue, BinLogEventParser.AllowType.Integer)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            convertNegativeInteger((Integer) oldValue, 4294967296L),
                            convertNegativeInteger((Integer) newValue, 4294967296L),
                            field.columnType,
                            field.isPrimary);
                }
                break;
            case UNSIGNED_TINYINT:
                if (checkTypes(oldValue, BinLogEventParser.AllowType.Integer)
                        &&
                        checkTypes(newValue, BinLogEventParser.AllowType.Integer)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            convertNegativeInteger((Integer) oldValue, 256L),
                            convertNegativeInteger((Integer) newValue, 256L),
                            field.columnType,
                            field.isPrimary);
                }
                break;
            case UNSIGNED_SMALLINT:
                if (checkTypes(oldValue, BinLogEventParser.AllowType.Integer)
                        &&
                        checkTypes(newValue, BinLogEventParser.AllowType.Integer)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            convertNegativeInteger((Integer) oldValue, 65536L),
                            convertNegativeInteger((Integer) newValue, 65536L),
                            field.columnType,
                            field.isPrimary);
                }
                break;
            case UNSIGNED_MEDIUMINT: {
                if (checkTypes(oldValue, BinLogEventParser.AllowType.Integer)
                        &&
                        checkTypes(newValue, BinLogEventParser.AllowType.Integer)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            convertNegativeInteger((Integer) oldValue, 16777216L),
                            convertNegativeInteger((Integer) newValue, 16777216L),
                            field.columnType,
                            field.isPrimary);
                }
                break;
            }
            case BIGINT:
            case UNSIGNED_BIGINT: {
                if (checkTypes(oldValue, BinLogEventParser.AllowType.Long)
                        &&
                        checkTypes(newValue, BinLogEventParser.AllowType.Long)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            }
            case DATE: {
                if (checkTypes(oldValue, BinLogEventParser.AllowType.SqlDate)
                        &&
                        checkTypes(newValue, BinLogEventParser.AllowType.SqlDate)) {
                    if (newValue == null) {
                        if (!field.nullable) {
                            if (oldValue == null)
                                return new SyncOperation.SyncItem<>(field.columnName,
                                        null,
                                        "0000-00-00",
                                        field.columnType,
                                        field.isPrimary);
                            else {
                                return new SyncOperation.SyncItem<>(field.columnName,
                                        oldValue.toString(),
                                        "0000-00-00",
                                        field.columnType,
                                        field.isPrimary);

                            }
                        }
                    }

                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            }
            case DATETIME: {
                if (checkTypes(oldValue, BinLogEventParser.AllowType.UtilDate)
                        &&
                        checkTypes(newValue, BinLogEventParser.AllowType.UtilDate)) {
                    Timestamp oldDate = null;
                    if (oldValue != null) {
                        oldDate = new Timestamp(((java.util.Date) oldValue).getTime());
                    }
                    Timestamp newDate = null;
                    if (newValue != null) {
                        newDate = new Timestamp(((java.util.Date) newValue).getTime());
                    }

                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldDate,
                            newDate,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            }
            case TIME:
            case YEAR: {
                if (checkTypes(oldValue, BinLogEventParser.AllowType.SqlDate)
                        &&
                        checkTypes(newValue, BinLogEventParser.AllowType.SqlDate)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            }
            case TIMESTAMP: {
                if (checkTypes(oldValue, BinLogEventParser.AllowType.TimeStamp)
                        &&
                        checkTypes(newValue, BinLogEventParser.AllowType.TimeStamp)) {
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
                if (checkTypes(oldValue, BinLogEventParser.AllowType.String)
                        &&
                        checkTypes(newValue, BinLogEventParser.AllowType.String)) {
                    return new SyncOperation.SyncItem<>(field.columnName,
                            oldValue,
                            newValue,
                            field.columnType,
                            field.isPrimary);
                }
                break;
            }
            default: {
                throw new IllegalArgumentException(String.format("Can't parse Illegal ColumnType of field %s", field.columnName));
            }
        }
        throw new IllegalArgumentException(String.format("Illegal value type to actual field %s", field.columnName));
    }

    private static Integer convertNegativeInteger(Integer integer, Long mask) {
        if (integer == null) return null;
        if (integer < 0) {
            return Long.valueOf(mask + integer).intValue();
        } else {
            return integer;
        }
    }

    private Boolean checkTypes(Serializable value, BinLogEventParser.AllowType dataType) {
        return value == null
                ||
                value.getClass().getName().equals(dataType.fullName);
    }

    private String transByteArrToStr(byte[] value) throws UnsupportedEncodingException {
        return new String(value, StandardCharsets.UTF_8);
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

}
