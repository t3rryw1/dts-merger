package com.cozystay.structure;

import com.cozystay.model.SyncTask;

public interface TaskRunner {
    void addTask(SyncTask newTask);

    void start();

    void workOn();

    void stop();

}
