package com.cozystay.model;


import java.util.List;
import java.util.Map;

public class SyncTaskImpl implements SyncTask{

    public final String database;
    public final String tableName;
    public final List<SyncItem> finalFields;
    public final List<List<SyncItem>> histories;

    public final Map<String,SyncTask.SyncStatus> statusMap;

    public final String source;
    public final String uuid;


    SyncTaskImpl(String source, String uuid, String database, String tableName, List<SyncItem> items) {
        this.database = database;
        this.tableName = tableName;


        finalFields = items;
        this.source = source;
        this.uuid = uuid;
        histories = null;
        statusMap = null;
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
        return null;
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
