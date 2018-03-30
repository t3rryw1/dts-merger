package com.cozystay.structure;

import com.cozystay.model.SyncTask;

public interface ProcessedTaskPool {
    void add(SyncTask task);

    void remove(SyncTask task);

    void remove(String taskId);

    boolean hasTask(SyncTask task);

    SyncTask poll();

    SyncTask get(String taskId);

}
