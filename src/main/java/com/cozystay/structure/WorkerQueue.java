package com.cozystay.structure;

import com.cozystay.model.SyncTask;

public interface WorkerQueue {
    public void addTask(SyncTask newTask);

    public void workOn(SyncTask task);

    public void start();

    public void stop();

}
