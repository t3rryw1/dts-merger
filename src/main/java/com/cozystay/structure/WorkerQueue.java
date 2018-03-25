package com.cozystay.structure;

import com.cozystay.model.SyncTask;

public interface WorkerQueue {
    void addTask(SyncTask newTask);

    void start();

    void workOn();

    void stop();

}
