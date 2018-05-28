package com.cozystay.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SyncTaskImpl implements SyncTask {

    private static Logger logger = LoggerFactory.getLogger(SyncTaskImpl.class);

    private final String database;
    private final String tableName;

    private final String uuid;

    private final List<SyncOperation> operations;

    SyncTaskImpl(){

        database = null;
        tableName = null;
        uuid = null;
        operations =new LinkedList<>();
    }

    public SyncTaskImpl(
            String uuid,
            String database,
            String tableName) {

        this.database = database;
        this.tableName = tableName;

        this.uuid = uuid;
        operations = new LinkedList<>();
    }

    @Override
    public String toString(){
        String operationStr = "";
        for(SyncOperation operation : this.operations){
            operationStr = operationStr + "\t" + operation.toString();
        }
        return String.format("id: %s; operations: %s",
                this.getId(),
                operationStr);
    }

    @Override
    public String getId() {
        return database +":"+ tableName +":"+ uuid;
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
    public boolean allOperationsCompleted() {
        for (SyncOperation operation : operations) {
            if (!operation.allSourcesCompleted()) {
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
                    selfOp.mergeStatus(toMergeOp);
                    continue toMerge;
                }
            }
            addOperation(toMergeOp);
        }
        Collections.sort(this.operations, new Comparator<SyncOperation>() {
            @Override
            public int compare(SyncOperation o1, SyncOperation o2) {
                if(!o1.getOperationType().equals(o2.getOperationType())){
                    return o1.getOperationType().compareTo(o2.getOperationType());
                    //
                }
                return o1.getTime()>(o2.getTime())?1:-1;
            }
        });
        return this;

    }

    public SyncTask mergeStatus(SyncTask task) {
        if(task.getOperations().size() > 1) {
            logger.error("in this case task should not contain multiple operations, task: {}", task.getId());
            return null;
        }

        SyncOperation toMergeOp = task.getOperations().get(0);
        for (SyncOperation operation : getOperations()) {
            if (toMergeOp.isSameOperation(operation)) {
                operation.mergeStatus(toMergeOp);
                break;
            }
        }
        return this;
    }

    public SyncTask deepMerge(SyncTask task) {
        SyncOperation toMergeOp = task.getOperations().get(0);
        List<SyncOperation> selfOps = getOperations();

        Boolean merged = false;
        for (SyncOperation selfOp : selfOps) {
            if (toMergeOp.getSource().equals(selfOp.getSource())){
                merged = true;
                selfOp.deepMerge(toMergeOp);
            }
        }
        if (!merged) {
            addOperation(toMergeOp);
        }
        return this;
    }

    public void addOperation(SyncOperation operation) {
        this.operations.add(operation);
        operation.setTask(this);
    }
}
