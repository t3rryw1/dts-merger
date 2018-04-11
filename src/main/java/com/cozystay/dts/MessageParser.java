package com.cozystay.dts;

import com.aliyun.drc.client.message.DataMessage;
import com.aliyun.drc.clusterclient.message.ClusterMessage;
import com.cozystay.model.SchemaRuleCollection;
import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;
import com.cozystay.model.SyncTaskBuilder;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class MessageParser {
    static SyncTask parseMessage(ClusterMessage message, String source, SchemaRuleCollection rules) {
        SyncTaskBuilder builder = SyncTaskBuilder.getInstance();
        DataMessage.Record record = message.getRecord();

        builder.setOperationType(getType(message.getRecord().getOpt()));

        builder.setDatabase(record.getDbname());

        builder.setTableName(record.getTablename());


        Long timestamp = Long.valueOf(record.getTimestamp());
        Date date = new Date(timestamp * 1000);
        builder.setOperationTime(date);

        builder.setSource(source);

        parseUuid(builder,record,rules);

        parseItems(builder, record);

        SyncTask newTask = builder.build();
        if (rules.filter(newTask.getOperations().get(0))) {
            return null;
        } else {
            return newTask;
        }
    }

    private static void parseUuid(SyncTaskBuilder builder, DataMessage.Record record, SchemaRuleCollection rules) {
        //TODO: read or generate uuid according to record and rules and set it to builder.
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
            DataMessage.Record.Field field = fields.get(i);
//            if (field.getFieldname().equals(record.getPrimaryKeys())) {
////                System.out.println(field.getFieldname()+" "+field.getValue().toString());
//                if(field.getValue()!=null){
//                    builder.setUuid(field.getValue().toString());
//                }
//            }

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
                if (newValue != null && newValue.equals(value)) {
                    return null;
                }
                try {
                    return buildItem(field, fieldName, value, newValue);
                } catch (Exception e) {
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
                                                    String newValue) throws ParseException {
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
                SimpleDateFormat sdfmt2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                return new SyncOperation.SyncItem<>(fieldName,
                        sdfmt2.parse(originValue),
                        sdfmt2.parse(newValue));
            case BIT:
                return new SyncOperation.SyncItem<>(fieldName,
                        Boolean.getBoolean(originValue),
                        Boolean.getBoolean(newValue));
            default:
                return null;
        }
    }
}