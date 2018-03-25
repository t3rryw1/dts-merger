package com.cozystay.structure;

import com.cozystay.model.SyncTask;

import java.util.concurrent.ConcurrentHashMap;

public  class SimpleProcessedTaskPool implements ProcessedTaskPool {
    ConcurrentHashMap<String, SyncTask> taskMap;

    public SimpleProcessedTaskPool() {
        taskMap = new ConcurrentHashMap<>();
    }

    @Override
    public void add(SyncTask task) {
        taskMap.put(task.getId(), task);
    }

    @Override
    public void remove(SyncTask task) {
        taskMap.remove(task.getId());
    }

    @Override
    public void remove(String taskId) {
        taskMap.remove(taskId);
    }

    @Override
    public boolean hasCollide(SyncTask task) {
        return false;
    }

    @Override
    public SyncTask poll() {
        return null;
    }

    @Override
    public SyncTask get(String taskId) {
        return taskMap.get(taskId);

    }
}
