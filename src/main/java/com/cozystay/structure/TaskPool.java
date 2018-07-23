package com.cozystay.structure;

import com.cozystay.model.SyncTask;

public interface TaskPool {
    void add(SyncTask task);

    void remove(SyncTask task);

    void remove(String taskId);

    boolean hasTask(SyncTask task);

    SyncTask poll();

    SyncTask peek();


    SyncTask get(String taskId);

    void destroy();

}
