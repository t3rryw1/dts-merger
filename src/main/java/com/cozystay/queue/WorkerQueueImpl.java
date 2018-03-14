package com.cozystay.queue;

import com.aliyun.drc.client.message.DataMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;

public abstract class WorkerQueueImpl implements WorkerQueue {
    Map<String, DataMessage.Record> processedRecords;
    ArrayBlockingQueue<DataMessage.Record> toDoTask;
    private Timer timer;


    WorkerQueueImpl() {
        processedRecords = new HashMap<>();
        toDoTask = new ArrayBlockingQueue<>(1000);
    }

    synchronized public void addTask(DataMessage.Record newRecord) {
        //TODO: remove dumb record
        //TODO: if find record in processedRecord?
        if (this.allowToAdd(newRecord)) {
            this.toDoTask.add(newRecord);
        }
    }

    public void start() {
        TimerTask timerTask = new TimerTask() {

            @Override
            public void run() {
                DataMessage.Record toProcess = toDoTask.poll();
                work(toProcess);
                //TODO: save toProcess In processedRecords;

            }
        };

        timer = new Timer("MyTimer");//create a new Timer

        timer.scheduleAtFixedRate(timerTask, 30, 3000);//this line starts the timer at the same time its executed
    }

    public void stop() {
        timer.cancel();
    }

    protected abstract boolean allowToAdd(DataMessage.Record newRecord);


    protected abstract void work(DataMessage.Record toProcess);
}
