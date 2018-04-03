package com.cozystay.model;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.drc.client.message.DataMessage;
import com.aliyun.drc.clusterclient.message.ClusterMessage;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class SyncTaskBuilder {
    private static SyncTaskBuilder builder;
    private static List<String> sourceList = new LinkedList<>();
    private String source;
    private Date operationTime;
    private List<SyncOperation.SyncItem> items;
    private String uuid;
    private String database;
    private String tableName;
    private SyncOperation.OperationType operationType;

    public void setSource(String source) {
        this.source = source;
    }

    public void setOperationTime(Date operationTime) {
        this.operationTime = operationTime;
    }

    public void addItem(SyncOperation.SyncItem newItem) {
        this.items.add(newItem);
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setOperationType(SyncOperation.OperationType operationType) {
        this.operationType = operationType;
    }


    private SyncTaskBuilder() {
    }

    void reset() {
        source = null;
        operationTime = null;
        items = new ArrayList<>();
        uuid = null;
        database = null;
        tableName = null;
        operationType = null;
    }

    public static SyncTaskBuilder getInstance() {
        if (builder == null) {
            builder = new SyncTaskBuilder();
        }
        builder.reset();
        return builder;
    }


    public SyncTask build() {
        if (this.source == null) {
            throw new IllegalArgumentException("source is null");
        }
        if (this.operationTime == null) {
            throw new IllegalArgumentException("operationTime is null");
        }
        if (this.items.size() == 0) {
            throw new IllegalArgumentException("items is empty");
        }
        if (this.uuid == null) {
            throw new IllegalArgumentException("uuid is null");
        }
        if (this.database == null) {
            throw new IllegalArgumentException("database is null");
        }
        if (this.tableName == null) {
            throw new IllegalArgumentException("tableName is null");
        }
        if (this.operationType == null) {
            throw new IllegalArgumentException("operationType is null");
        }
        SyncTask task = new SyncTaskImpl(this.uuid,
                this.database,
                this.tableName);
        SyncOperation operation = new SyncOperationImpl(task,
                this.operationType,
                this.items,
                this.source,
                sourceList,
                this.operationTime);
        task.getOperations().add(operation);
        return task;
//        DataMessage.Record record = message.getRecord();

//        String key = record.getPrimaryKeys();
//        DataMessage.Record.Type operation = record.getOpt();
//        List<SyncTask.SyncItem> updates = compareUpdateFields();
//        SyncTaskImpl task= new SyncTaskImpl(source,key,record.getDbname(),record.getTablename(),operation,updates);
    }

    public static void addSource(String name) {
        sourceList.add(name);
    }



    private void convertRecord(ClusterMessage message) throws UnsupportedEncodingException {
        DataMessage.Record record = message.getRecord();
        System.out.println("Record Op type:" + record.getOpt().toString());
        JSONObject jsonRecord;
        String key = null;
        switch (record.getOpt()) {
            case INSERT: // 数据插入
                jsonRecord = convertFields(record, 0, 1);
                key = record.getPrimaryKeys();
                System.out.println("Record Insert:Json format:" + jsonRecord.toJSONString());
                break;
            case UPDATE:// 数据更新
            case REPLACE:// replace操作
                JSONObject oldJsonRecord = convertFields(record, 0, 2);
                System.out.println("Record Update Before:Json format:" + oldJsonRecord.toJSONString());
                jsonRecord = convertFields(record, 1, 2);
                System.out.println("Record Update Before:Json format:" + jsonRecord.toJSONString());
                key = record.getPrimaryKeys();
                break;
            case DELETE:// 数据删除
                jsonRecord = convertFields(record, 0, 1);
                System.out.println("Record Delete:Json format:" + jsonRecord.toJSONString());
                key = record.getPrimaryKeys();
                break;
            default:
                return;
        }
        //数据表中对主Key列名
        System.out.println("PrimaryKey Column Name:" + key);
        //drds中物理数据库名和物理数据表名
        System.out.println("Record DB Name:" + record.getDbname() + ",Table Name:" + record.getTablename());
        //drds中逻辑数据库名和逻辑表名
//		System.out.println("Record Logical DB Name:"+record.getLogicalDbname()+",Table Name:"+record.getLogicalTablename());

    }

    // 将消息组成JSON格式输出
    private JSONObject convertFields(DataMessage.Record record, int start, int step)
            throws UnsupportedEncodingException {
        List<DataMessage.Record.Field> fields = record.getFieldList();
        JSONObject ret = new JSONObject();
        for (int i = start; i < fields.size(); i += step) {
            DataMessage.Record.Field field = fields.get(i);
            JSONObject object = new JSONObject();
            object.put("type", field.getType().toString());
            object.put("encoding", field.getEncoding());
            if (field.getValue() != null) {
                object.put("value", field.getValue().toString(field.getEncoding()));
            } else {
                object.put("value", null);
            }
            ret.put(field.getFieldname(), object);
        }
        return ret;
    }
}
