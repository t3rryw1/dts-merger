package com.cozystay.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SyncOperationImpl implements SyncOperation {
    private SyncTask task;
    private final OperationType operationType;
    private List<SyncItem> syncItems;
    private final Map<String, SyncStatus> syncStatusMap;

    public SyncOperationImpl(SyncTask task,
                             OperationType operationType,
                             List<SyncItem> syncItems,
                             String source,
                             List<String> sourceList) {
        this.task = task;
        this.operationType = operationType;
        this.syncItems = syncItems;
        this.syncStatusMap = new HashMap<>();
        for (String sourceName : sourceList) {
            syncStatusMap.put(sourceName, SyncStatus.INITED);
        }
        if (!syncStatusMap.containsKey(source)) {
            throw new IllegalArgumentException("Source " + source + " is not in Source list");
        }
        syncStatusMap.put(source, SyncStatus.COMPLETED);
    }

    @Override
    public SyncTask getTask() {
        return this.task;
    }

    @Override
    public OperationType getOperationType() {
        return operationType;
    }

    @Override
    public List<SyncItem> getSyncItems() {
        return syncItems;
    }

    @Override
    public Map<String, SyncStatus> getSyncStatus() {
        return syncStatusMap;
    }

    @Override
    public String buildSql() {
        //TODO: impl
        return null;
    }

    @Override
    public boolean isSameOperation(SyncOperation operation) {
        if (!operation.getOperationType().equals(this.getOperationType()))
            return false;
        if (!operation.getSyncItems().containsAll(this.getSyncItems())) {
            return false;
        }
        if (!this.getSyncItems().containsAll(operation.getSyncItems())) {
            return false;
        }
        return true;
    }

    @Override
    public void updateStatus(String source, SyncStatus status) {
        this.syncStatusMap.put(source, status);

    }

    @Override
    public boolean shouldSendToSource(String name) {
        return this.syncStatusMap.get(name).equals(SyncStatus.INITED);
    }

    @Override
    public void setSourceSend(String name) {
        this.updateStatus(name, SyncStatus.SEND);
    }

    @Override
    public void merge(SyncOperation toMergeOp) {

    }

    @Override
    public boolean completedAllSources() {
        List<SyncStatus> list = new ArrayList<>(syncStatusMap.values());
        for (SyncStatus status : list) {
            if (status != SyncStatus.COMPLETED) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void setTask(SyncTask syncTask) {
        this.task = syncTask;

    }

    @Override
    public boolean collideWith(SyncOperation selfOp) {
        List<SyncItem> toCompareItems = selfOp.getSyncItems();
        List<SyncItem> myItems = getSyncItems();

        for (SyncItem toCompareItem : toCompareItems) {
            for (SyncItem myItem : myItems) {
                if (toCompareItem.fieldName.equals(myItem.fieldName))
                    return true;
            }
        }
        return false;
    }

    @Override
    public SyncOperation resolveCollide(SyncOperation toMergeOp) {
        return null;
    }
}
