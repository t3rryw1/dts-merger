package com.cozystay.model;


import java.util.LinkedList;
import java.util.List;

public class SyncTaskImpl implements SyncTask {

    public final String database;
    public final String tableName;

    public final String uuid;

    public final List<SyncOperation> operations;


    SyncTaskImpl(
            String uuid,
            String database,
            String tableName) {

        this.database = database;
        this.tableName = tableName;

        this.uuid = uuid;
        operations = new LinkedList<>();
    }


    @Override
    public String getId() {
        return database + tableName + uuid;
    }

    @Override
    public String getDatabase() {
        return this.database;
    }


    @Override
    public String getTable() {
        return this.tableName;
    }

    @Override
    public List<SyncOperation> getOperations() {
        return this.operations;
    }

    @Override
    public boolean completeAllOperations() {
        for (SyncOperation operation : operations) {
            if (!operation.completedAllSources()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public SyncTask merge(SyncTask task) {
        if (!task.getId().equals(this.getId())) {
            return null;
        }
        List<SyncOperation> toMergeOps = task.getOperations();
        List<SyncOperation> selfOps = getOperations();

        toMerge:
        for (SyncOperation toMergeOp : toMergeOps) {
            for (SyncOperation selfOp : selfOps) {
                if (toMergeOp.isSameOperation(selfOp)) {
                    selfOp.merge(toMergeOp);
                    continue toMerge;
                }
                if(toMergeOp.collideWith(selfOp)){
                    addOperation(selfOp.resolveCollide(toMergeOp));
                    continue toMerge;
                }
            }
            addOperation(toMergeOp);
        }
        return this;

    }

    void addOperation(SyncOperation operation) {
        this.operations.add(operation);
        operation.setTask(this);
    }
}
