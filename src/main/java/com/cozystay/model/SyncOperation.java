package com.cozystay.model;

import java.util.*;

public interface SyncOperation {
    String toString();

    SyncTask getTask();

    Long getTime();

    OperationType getOperationType();

    List<SyncItem> getSyncItems();

    String getSource();

    Map<String, SyncStatus> getSyncStatus();

    String buildSql();

    boolean isSameOperation(SyncOperation operation);

    void updateStatus(String source, SyncStatus status);

    void reduceItems();

    boolean shouldSendToSource(String name);

    void setSourceSend(String name);

    void mergeStatus(SyncOperation toMergeOp);

    SyncOperation deepMerge(SyncOperation toMergeOp);

    boolean allSourcesCompleted();

    void setTask(SyncTask syncTask);

    boolean collideWith(SyncOperation selfOp);

    SyncOperation resolveCollide(SyncOperation toMergeOp);


    enum SyncStatus {
        INIT,
        SEND,
        COMPLETED
    }

    enum OperationType {
        CREATE,
        UPDATE,
        REPLACE,
        DELETE
    }


}
