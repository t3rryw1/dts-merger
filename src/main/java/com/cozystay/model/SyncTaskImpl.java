package com.cozystay.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class SyncTaskImpl implements SyncTask {

    private static Logger logger = LoggerFactory.getLogger(SyncTaskImpl.class);

    private final String database;
    private final String tableName;

    private final String uuid;

    private final SyncOperation.OperationType type;

    private final List<SyncOperation> operations;

    SyncTaskImpl() {

        database = null;
        tableName = null;
        uuid = null;
        type = SyncOperation.OperationType.CREATE;
        operations = new LinkedList<>();

    }

    public SyncTaskImpl(
            String uuid,
            String database,
            String tableName,
            SyncOperation.OperationType type) {

        this.database = database;
        this.tableName = tableName;

        this.uuid = uuid;
        operations = new LinkedList<>();
        this.type = type;
    }

    @Override
    public String toString() {
        String operationStr = "";
        for (SyncOperation operation : this.operations) {
            operationStr = operationStr + "\t" + operation.toString();
        }
        return String.format("id: %s; operations: %s",
                this.getId(),
                operationStr);
    }

    @Override
    public String getId() {
        return database + ":"
                + tableName + ":"
                + type + ":"
                + uuid;
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
    public boolean canMergeStatus(SyncOperation toMergeOp) {

        for (SyncOperation operation : getOperations()) {
            if (toMergeOp.isSameOperation(operation)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public SyncOperation firstOperation() {
        if (getOperations().size() < 1) {
            return null;
        }
        return getOperations().get(0);
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
                if (!o1.getOperationType().equals(o2.getOperationType())) {
                    return o1.getOperationType().compareTo(o2.getOperationType());
                    //
                }
                return o1.getTime() > (o2.getTime()) ? 1 : -1;
            }
        });
        return this;

    }

    public SyncTask mergeStatus(SyncTask task) {
        if (task.getOperations().size() > 1) {
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
        List<SyncOperation> allOps = new ArrayList<>();
        allOps.addAll(task.getOperations());
        allOps.addAll(getOperations());

        Map<String, List<SyncOperation>> sources = new HashMap<>();

        for (SyncOperation operation : allOps) {
            String sourceName = operation.getSource();
            if(sources.containsKey(sourceName)){
                sources.get(sourceName).add(operation);
            }else{
                sources.put(sourceName, new ArrayList<>(Collections.singletonList(operation)));
            }
        }

        SyncTask mergedTask = new SyncTaskImpl(uuid, database, tableName, type);
        List<SyncOperation> diffOps = new ArrayList<>();

        for (Map.Entry<String, List<SyncOperation>> source : sources.entrySet()) {
            SyncOperation operation = deepMergeOps(source.getValue());
            diffOps.add(operation);
        }

        for (SyncOperation operation : fixConflictOpsFromDiffSource(diffOps)) {
            mergedTask.addOperation(operation);
        }

        return mergedTask;
    }

    private SyncOperation deepMergeOps(List<SyncOperation> Ops) {
        Ops.sort((o1, o2) -> o1.getTime() > (o2.getTime()) ? -1 : 1);

        SyncOperation acc = null;
        //reduce
        for (SyncOperation operation : Ops) {
            if(acc == null){
                acc = operation;
                continue;
            }
            acc = acc.deepMerge(operation);
        }
        return acc;
    }

    private List<SyncOperation> fixConflictOpsFromDiffSource(List<SyncOperation> diffOps) {
        diffOps.sort((o1, o2) -> o1.getTime() > (o2.getTime()) ? -1 : 1);

        Map<String, Integer> fieldCount = new HashMap<>();
        for (SyncOperation operation : diffOps) {
            for (SyncOperation.SyncItem item : operation.getSyncItems()) {
                if (fieldCount.containsKey(item.fieldName)) {
                    fieldCount.put(item.fieldName, fieldCount.get(item.fieldName) + 1);
                } else {
                    fieldCount.put(item.fieldName, 1);
                }
            }
        }

        for (Map.Entry<String, Integer> field : fieldCount.entrySet()) {
            if (field.getValue() > 1) {
                //if fieldName count more than once，it means operation which contain this fieldName should be merged
                List<SyncOperation> needMergeOps = new ArrayList<>();
                Iterator<SyncOperation> iterativelyOps = diffOps.iterator();
                while (iterativelyOps.hasNext()) {
                    SyncOperation operation = iterativelyOps.next();
                    if (operation.getSyncItems().stream().anyMatch(syncItem -> syncItem.fieldName == field.getKey())) {
                        //remove conflict operation
                        iterativelyOps.remove();

                        //add to be merged
                        needMergeOps.add(operation);
                    }
                }
                //merge this conflict operations and put it back
                diffOps.add(deepMergeOps(needMergeOps));
            }
        }

        return diffOps;
    }

    public void addOperation(SyncOperation operation) {
        this.operations.add(operation);
        operation.setTask(this);
    }
}
