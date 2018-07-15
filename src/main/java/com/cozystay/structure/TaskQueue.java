package com.cozystay.structure;

import com.cozystay.model.SyncTask;

public interface TaskQueue {
    void push(SyncTask task);

    SyncTask pop();

    int size();

    void destroy();

}
