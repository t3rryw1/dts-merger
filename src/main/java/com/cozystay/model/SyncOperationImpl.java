package com.cozystay.model;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.util.*;

public class SyncOperationImpl implements SyncOperation {
    private SyncTask task;
    private final OperationType operationType;
    private List<SyncItem> syncItems;
    private final Map<String, SyncStatus> syncStatusMap;
    private final Date operationTime;


    SyncOperationImpl() {

        operationType = null;
        syncItems = new ArrayList<>();
        syncStatusMap = new HashMap<>();
        operationTime = null;
    }

    public SyncOperationImpl(SyncTask task,
                             OperationType operationType,
                             List<SyncItem> syncItems,
                             String source,
                             List<String> sourceList,
                             Date operationTime) {
        this.task = task;
        this.operationType = operationType;
        this.syncItems = syncItems;
        this.operationTime = operationTime;
        this.syncStatusMap = new HashMap<>();
        for (String sourceName : sourceList) {
            syncStatusMap.put(sourceName, SyncStatus.INIT);
        }
        if (source != null) {
            if (!syncStatusMap.containsKey(source)) {
                throw new IllegalArgumentException(String.format("Source %s is not in Source list", source));
            }
            syncStatusMap.put(source, SyncStatus.COMPLETED);
        }
    }

    @Override
    public String toString() {
        return String.format("operationType: %s; status: %s; sql: %s time: %s",
                this.getOperationType(),
                this.getSyncStatus().toString(),
                this.buildSql(),
                this.getTime());
    }

    @Override
    public SyncTask getTask() {
        return this.task;
    }

    @Override
    public Date getTime() {
        return this.operationTime;
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
        //impl
        switch (getOperationType()) {
            case DELETE:
                String conditionString = getConditionString();
                if (conditionString == null) {
                    return null;
                }
                return String.format("DELETE FROM %s WHERE %s;", getTask().getTable(), conditionString);
            case CREATE:
                List<String> keys = new ArrayList<>(), values = new ArrayList<>();
                for (SyncItem item : getSyncItems()) {
                    if (item.currentValue != null) {
                        keys.add(item.fieldName);
                        values.add(item.currentValueToString());
                    }
                }
                if (values.size() == 0) {
                    return null;
                }
                return String.format("INSERT INTO %s (%s) VALUES (%s);",
                        getTask().getTable(),
                        StringUtils.join(keys, ", "),
                        StringUtils.join(values, ", "));
            case UPDATE:
            case REPLACE:
                conditionString = getConditionString();
                if (conditionString == null) {
                    return null;
                }
                List<String> operations = new ArrayList<>();
                for (SyncItem item : getSyncItems()) {
                    if (!item.hasChange()) {
                        continue;
                    }
                    if (item.currentValue == null) {
                        operations.add(String.format(" %s = %s ", item.fieldName, "NULL"));
                    } else {
                        operations.add(String.format(" %s = %s ", item.fieldName, item.currentValueToString()));

                    }
                }
                if (operations.size() == 0) {
                    return null;
                }
                return String.format("UPDATE %s SET %s WHERE %s;",
                        getTask().getTable(),
                        StringUtils.join(operations, ", "),
                        conditionString);

        }
        return null;
    }


    private String getConditionString() {
        List<String> conditions = new ArrayList<>();
        for (SyncItem item : getSyncItems()) {
            if (!item.isIndex) {
                continue;
            }
            if (item.originValue == null) {
                return null;
            }
            conditions.add(String.format(" %s = %s ", item.fieldName, item.originValueToString()));
        }
        if (conditions.size() == 0) {
            return null;
        }
        return StringUtils.join(conditions.toArray(), " and ");
    }

    @Override
    public boolean isSameOperation(SyncOperation operation) {
        if (!operation.getOperationType().equals(this.getOperationType()))
            return false;
        //if both operation are to delete the item, consider them the same operation
        if (operation.getOperationType().equals(OperationType.DELETE))
            return true;
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
        return this.syncStatusMap.get(name).equals(SyncStatus.INIT);
    }

    @Override
    public void setSourceSend(String name) {
        this.updateStatus(name, SyncStatus.SEND);
    }

    @Override
    public void mergeStatus(SyncOperation toMergeOp) {
        List<String> keys = new ArrayList<>(syncStatusMap.keySet());
        for (String key : keys) {
            SyncStatus status = toMergeOp.getSyncStatus().get(key);
            if (status.compareTo(this.syncStatusMap.get(key)) > 0) {
                this.syncStatusMap.put(key, status);
            }
        }

    }

    @Override
    public boolean allSourcesCompleted() {
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
    public boolean collideWith(SyncOperation toCompareOp) {
        if (!toCompareOp.getOperationType().equals(this.getOperationType()))
            return false;

        List<SyncItem> toCompareItems = toCompareOp.getSyncItems();
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
        if (!toMergeOp.getOperationType().equals(this.getOperationType()))
            return null;
        OperationType operationType = toMergeOp.getOperationType();
        List<SyncItem> selfItems = this.getSyncItems();
        List<SyncItem> toMergeItems = toMergeOp.getSyncItems();
        List<SyncItem> mergedItems = new ArrayList<>(selfItems);
//        Collections.copy(mergedItems,toMergeItems);
        nextToMergeItem:
        for (SyncItem toMergeItem : toMergeItems) {
            for (SyncItem selfItem : selfItems) {
                if (selfItem.fieldName.equals(toMergeItem.fieldName)) {
                    //found item with same field, check timestamp,
                    //replace item with newer one if necessary
                    if (toMergeOp.getTime().after(this.getTime())) {
                        mergedItems.remove(selfItem);
                        mergedItems.add(toMergeItem);
                    }
                    //if replaced, go to next toMergeItem
                    continue nextToMergeItem;
                }
            }
            //if no match found in selfItems, add the item to itemList
            mergedItems.add(toMergeItem);
        }
        Date operationTime = toMergeOp.getTime().after(this.getTime())
                ?
                toMergeOp.getTime()
                :
                getTime();
        operationTime = DateUtils.addSeconds(operationTime, 1);
        List<String> sourceList = new ArrayList<>(syncStatusMap.keySet());

        return new SyncOperationImpl(
                this.getTask(),
                operationType,
                mergedItems,
                null,
                sourceList,
                operationTime
        );
    }
}
