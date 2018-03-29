package com.cozystay.model;


import com.aliyun.drc.client.message.DataMessage;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SyncTaskImpl implements SyncTask{

    public final String database;
    public final String tableName;
    public final List<SyncItem> finalFields;
    public final List<SyncTask> histories;

    public final Map<String,SyncTask.SyncStatus> statusMap;

    public final String source;
    public final String uuid;

    private DataMessage.Record.Type operationType;


    SyncTaskImpl(String source,
                 String uuid,
                 String database,
                 String tableName,
                 DataMessage.Record.Type operationType,
                 List<SyncItem> items) {

        this.database = database;
        this.tableName = tableName;

        finalFields = items;
        this.source = source;
        this.uuid = uuid;
        this.operationType = operationType;
        histories = new LinkedList<>();
        statusMap = new HashMap<>();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String getId(){
        return database+tableName+uuid;
    }

    @Override
    public String getDatabase() {
        return this.database;
    }

    @Override
    public String buildSql() {
        return null;
    }

    @Override
    public boolean shouldWriteSource(String source) {
        return false;
    }

    @Override
    public void setSourceWritten(String name) {

    }


    @Override
    public boolean hasDone(SyncTask newRecord) {
        return false;
    }

    @Override
    public String getSource() {
        return null;
    }

    @Override
    public void setSourceFinished(String source) {

    }

    @Override
    public boolean allSourcesFinished() {
        return false;
    }

    @Override
    public boolean contains(SyncTask task){
        return false;
    }

    @Override
    public SyncTaskImpl merge(SyncTask task){
        return null;
    }
}
