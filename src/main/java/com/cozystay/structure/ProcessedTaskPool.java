package com.cozystay.structure;

import com.cozystay.model.SyncTask;

public interface ProcessedTaskPool {
    void add(SyncTask task);
    void remove(SyncTask task);
    void remove(String taskId);
    boolean hasDuplicateTask(SyncTask task);
    boolean hasCollideTask(SyncTask task);
    SyncTask merge(SyncTask task);
    SyncTask get(String taskId);

}
