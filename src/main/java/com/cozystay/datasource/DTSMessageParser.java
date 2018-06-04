package com.cozystay.datasource;

import com.aliyun.drc.client.message.DataMessage;
import com.aliyun.drc.clusterclient.message.ClusterMessage;
import com.cozystay.db.schema.SchemaRuleCollection;
import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;
import com.cozystay.model.SyncTaskBuilder;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.slf4j.*;
public class DTSMessageParser {
    private static Logger LOGGER = LoggerFactory.getLogger(DTSMessageParser.class);

    private static final SimpleDateFormat defaultDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    static SyncTask parseMessage(ClusterMessage message,
                                 String source,
                                 SchemaRuleCollection rules)
            throws UnsupportedEncodingException {
        SyncTaskBuilder builder = new SyncTaskBuilder();
        DataMessage.Record record = message.getRecord();

        builder.setOperationType(getType(message.getRecord().getOpt()));

        builder.setDatabase(record.getDbname());

        builder.setTableName(record.getTablename());


        Long timestamp = Long.valueOf(record.getTimestamp());
        builder.setOperationTime(timestamp * 1000);

        builder.setSource(source);

        parseUuid(builder, record, rules);

        parseItems(builder, record);

        SyncTask newTask = builder.build();

        LOGGER.info("create task: {}", newTask.toString());

        if (rules.filter(newTask.getOperations().get(0))) {
            return null;
        } else {
            return newTask;
        }
    }


    private static void parseUuid(SyncTaskBuilder builder,
                                  DataMessage.Record record,
                                  SchemaRuleCollection rules)
            throws UnsupportedEncodingException {

        // read or generate uuid according to record and rules and set it to builder.
        List<DataMessage.Record.Field> fields = record.getFieldList();
        UuidBuilder uuidBuilder = new UuidBuilder();
        List<String> idFields;
        if (record.getPrimaryKeys() != null) {
            idFields = Arrays.asList(record.getPrimaryKeys().split(","));
        } else {
            idFields = rules.getPrimaryKeys(record.getDbname(), record.getTablename());

        }
        if (idFields == null) {
            throw new IllegalArgumentException(
                    String.format("schema index configuration error, need config for db: %s, table: %s",
                            record.getDbname(),
                            record.getTablename()));
        }
        for (DataMessage.Record.Field field : fields) {
            if (idFields.contains(field.getFieldname())) {
                field.setPrimary(true);
                uuidBuilder.addValue(field.getValue().toString(field.getEncoding()));
            } else {
                field.setPrimary(false);
            }
        }

        builder.setUuid(uuidBuilder.build());
    }

    private static SyncOperation.OperationType getType(DataMessage.Record.Type opt) {
        switch (opt) {
            case INSERT:
                return SyncOperation.OperationType.CREATE;
            case UPDATE:
                return SyncOperation.OperationType.UPDATE;
            case REPLACE:
                return SyncOperation.OperationType.REPLACE;
            case DELETE:
                return SyncOperation.OperationType.DELETE;
            default:
                return null;
        }
    }

    private static void parseItems(SyncTaskBuilder builder, DataMessage.Record record) {
        int step = 1;
        switch (record.getOpt()) {
            case UPDATE:
            case REPLACE:
                step = 2;
                break;
            default:
                break;
        }
        List<DataMessage.Record.Field> fields = record.getFieldList();
        for (int i = 0; i < fields.size(); i += step) {

            SyncOperation.SyncItem item;
            try {
                item = parseItem(fields, i, record.getOpt());
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                item = null;
            }
            if (item != null) {
                builder.addItem(item);
            }
        }
    }

    private static SyncOperation.SyncItem parseItem(List<DataMessage.Record.Field> fields,
                                                    int start,
                                                    DataMessage.Record.Type type)
            throws UnsupportedEncodingException {
        DataMessage.Record.Field field = fields.get(start);
        String fieldName = field.getFieldname();
        String value;
        if (field.getValue() != null) {
            value = field.getValue().toString(field.getEncoding());
        } else {
            value = null;
        }
        switch (type) {
            case INSERT:
                try {
                    return buildItem(field, fieldName, null, value);
                } catch (Exception e) {
                    return null;
                }

            case UPDATE:
            case REPLACE:
                DataMessage.Record.Field newField = fields.get(start + 1);
                String newValue;
                if (newField.getValue() != null) {
                    newValue = newField.getValue().toString(field.getEncoding());
                } else {
                    newValue = null;
                }
                if (newValue == null && value == null) {
                    return null;
                }
                if (newValue != null && newValue.equals(value) && !newField.isPrimary()) {
                    return null;
                }
                try {
                    return buildItem(field, fieldName, value, newValue);
                } catch (ParseException e) {
                    return null;
                }
            case DELETE:
                try {
                    return buildItem(field, fieldName, null, value);
                } catch (Exception e) {
                    return null;
                }

            default:
                return null;

        }
    }

    private static SyncOperation.SyncItem buildItem(DataMessage.Record.Field field,
                                                    String fieldName,
                                                    String originValue,
                                                    String newValue)
            throws ParseException {
        switch (field.getType()) {
            case STRING:
            case JSON:
            case ENUM:
                return new SyncOperation.SyncItem<>(fieldName,
                        originValue,
                        newValue,
                        SyncOperation.SyncItem.ColumnType.CHAR,
                        field.isPrimary());
            case INT8:
            case INT16:
            case INT24:
            case INT32:
            case INT64:
                return new SyncOperation.SyncItem<>(fieldName,
                        Integer.valueOf(originValue),
                        Integer.valueOf(newValue),
                        SyncOperation.SyncItem.ColumnType.INT,
                        field.isPrimary());
            case DECIMAL:
            case FLOAT:
            case DOUBLE:
                return new SyncOperation.SyncItem<>(fieldName,
                        Integer.valueOf(originValue),
                        Integer.valueOf(newValue),
                        SyncOperation.SyncItem.ColumnType.DECIMAL,
                        field.isPrimary());
            case DATE:
            case DATETIME:
            case TIME:
                return new SyncOperation.SyncItem<>(fieldName,
                        Date.parse(originValue),
                        Date.parse(newValue),
                        SyncOperation.SyncItem.ColumnType.DATE,
                        field.isPrimary());
            case NULL:
                break;
            case TIMESTAMP:
                return new SyncOperation.SyncItem<>(fieldName,
                        defaultDateFormat.parse(originValue),
                        defaultDateFormat.parse(newValue),
                        SyncOperation.SyncItem.ColumnType.TIMESTAMP,
                        field.isPrimary());
            case YEAR:
                break;
            case BIT:
                return new SyncOperation.SyncItem<>(fieldName,
                        Boolean.getBoolean(originValue),
                        Boolean.getBoolean(newValue),
                        SyncOperation.SyncItem.ColumnType.BIT,
                        field.isPrimary());
            case SET:
                break;
            case BLOB:
                break;
            case GEOMETRY:
                break;
            case UNKOWN:
                break;
        }
        return null;
    }

}
