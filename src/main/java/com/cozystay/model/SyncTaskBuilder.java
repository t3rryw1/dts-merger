package com.cozystay.model;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.drc.client.message.DataMessage;
import com.aliyun.drc.clusterclient.message.ClusterMessage;

import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;

public class SyncTaskBuilder {
    private static List<String> sourceList = new LinkedList<>();
    public static SyncTaskImpl build(ClusterMessage message, String source) {
        //TODO: reformat message to SyncTaskImpl
        DataMessage.Record record = message.getRecord();

        String key = record.getPrimaryKeys();
        DataMessage.Record.Type operation = record.getOpt();
        List<SyncTask.SyncItem> updates = compareUpdateFields();
        SyncTaskImpl task= new SyncTaskImpl(source,key,record.getDbname(),record.getTablename(),operation,updates);
    }

    private static List<SyncTask.SyncItem> compareUpdateFields() {
        return null;
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
        System.out.println("Record DB Name:"+record.getDbname()+",Table Name:"+record.getTablename());
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
