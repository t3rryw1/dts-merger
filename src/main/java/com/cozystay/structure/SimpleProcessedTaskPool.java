package com.cozystay.structure;

import com.cozystay.model.SyncTask;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SimpleProcessedTaskPool implements ProcessedTaskPool {
    private ConcurrentHashMap<String, SyncTask> taskMap;
    private ConcurrentLinkedQueue<String> idQueue;


    public SimpleProcessedTaskPool() {
        taskMap = new ConcurrentHashMap<>();
        idQueue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void add(SyncTask task) {
        taskMap.put(task.getId(), task);
        idQueue.add(task.getId());
    }

    @Override
    public void remove(SyncTask task) {
        taskMap.remove(task.getId());
        idQueue.remove(task.getId());
    }

    @Override
    public void remove(String taskId) {
        taskMap.remove(taskId);
        idQueue.remove(taskId);
    }

    @Override
    public boolean hasTask(SyncTask task) {
        return taskMap.containsKey(task.getId());
    }

    @Override
    public SyncTask poll() {
        String taskId = idQueue.poll();
        if(taskId!=null){
            return taskMap.remove(taskId);
        }else{
            return null;
        }
    }

    @Override
    public SyncTask get(String taskId) {
        return taskMap.get(taskId);
    }
}
