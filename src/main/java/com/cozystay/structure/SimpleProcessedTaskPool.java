package com.cozystay.structure;

import com.cozystay.model.SyncTask;

import java.util.concurrent.ConcurrentHashMap;

public abstract class SimpleProcessedTaskPool implements ProcessedTaskPool {
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
    public boolean hasCollideTask(SyncTask task) {
        return taskMap.containsKey(task.getId());
    }

    @Override
    public SyncTask get(String taskId) {
        return taskMap.get(taskId);

    }
}
