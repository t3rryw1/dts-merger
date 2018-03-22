package com.cozystay.model;


import java.util.List;

public class SyncTask {
    public final String database;
    public final String tableName;
    public final List<SyncItem> updateFields;
    public final String source;
    public final String uuid;

    public String getId(){
        return database+tableName+uuid;
    }

    SyncTask(String source, String uuid, String database, String tableName, List<SyncItem> items) {
        this.database = database;
        this.tableName = tableName;

        updateFields = items;
        this.source = source;
        this.uuid = uuid;
    }

}
