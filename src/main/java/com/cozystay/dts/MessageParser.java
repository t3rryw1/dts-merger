package com.cozystay.dts;

import com.aliyun.drc.client.message.DataMessage;
import com.aliyun.drc.clusterclient.message.ClusterMessage;
import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;
import com.cozystay.model.SyncTaskBuilder;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;

public class MessageParser {
    static SyncTask parseMessage(ClusterMessage message, String source) {
        SyncTaskBuilder builder = SyncTaskBuilder.getInstance();
        DataMessage.Record record = message.getRecord();

        builder.setOperationType(getType(message.getRecord().getOpt()));

        builder.setDatabase(record.getDbname());

        builder.setTableName(record.getTablename());

        builder.setUuid(record.getPrimaryKeys());

        Long timestamp = Long.getLong(record.getTimestamp());
        Date date = new Date(timestamp);
        builder.setOperationTime(date);

        builder.setSource(source);

        parseItems(builder, record);


        return builder.build();
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

    private static SyncOperation.SyncItem parseItem(List<DataMessage.Record.Field> fields, int start, DataMessage.Record.Type type)
            throws UnsupportedEncodingException {
        DataMessage.Record.Field field = fields.get(start);
        String fieldName = field.getFieldname();
        String value = field.getValue().toString(field.getEncoding());
        switch (type) {
            case INSERT:
                return buildItem(field, fieldName, null, value);
            case UPDATE:
            case REPLACE:
                DataMessage.Record.Field newField = fields.get(start + 1);
                String newValue = newField.getValue().toString(field.getEncoding());
                if (newValue.equals(value)) {
                    return null;
                }
                return buildItem(field, fieldName, value, newValue);
            case DELETE:
                return buildItem(field, fieldName, null, value);
            default:
                return null;

        }
    }

    private static SyncOperation.SyncItem buildItem(DataMessage.Record.Field field,
                                                    String fieldName,
                                                    String originValue,
                                                    String newValue) {
        switch (field.getType()) {
            case STRING:
            case JSON:
            case ENUM:
                return new SyncOperation.SyncItem<>(fieldName,
                        originValue,
                        newValue);
            case INT8:
            case INT16:
            case INT24:
            case INT32:
            case INT64:
            case DECIMAL:
                return new SyncOperation.SyncItem<>(fieldName,
                        Integer.valueOf(originValue),
                        Integer.valueOf(newValue));
            case FLOAT:
            case DOUBLE:
                return new SyncOperation.SyncItem<>(fieldName,
                        Double.valueOf(originValue),
                        Double.valueOf(newValue));
            case DATE:
            case DATETIME:
            case TIME:
                return new SyncOperation.SyncItem<>(fieldName,
                        Date.parse(originValue),
                        Date.parse(newValue));
            case TIMESTAMP:
                return new SyncOperation.SyncItem<>(fieldName,
                        new Date(Long.valueOf(originValue)),
                        new Date(Long.valueOf(newValue)));
            case BIT:
                return new SyncOperation.SyncItem<>(fieldName,
                        Boolean.getBoolean(originValue),
                        Boolean.getBoolean(newValue));
            default:
                return null;
        }
    }
}
