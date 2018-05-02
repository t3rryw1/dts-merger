package com.cozystay.model;

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
        if(!this.items.contains(newItem)){
            this.items.add(newItem);
        }
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

    private void reset() {
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
    }

    public static void addSource(String name) {
        sourceList.add(name);
    }

}
