package com.cozystay.structure;

import com.cozystay.model.SyncTask;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;

public abstract class SimpleWorkerQueueImpl implements WorkerQueue {
    private final int delay;
    private final int interval;
    private ArrayBlockingQueue<SyncTask> toDoTask;
    private Timer timer;


    public SimpleWorkerQueueImpl(int delay, int interval) {

        toDoTask = new ArrayBlockingQueue<>(1000);
        this.delay = delay;
        this.interval = interval;
    }

    synchronized public void addTask(SyncTask newRecord) {
        this.toDoTask.add(newRecord);
    }

    public void start() {
        TimerTask timerTask = new TimerTask() {

            @Override
            public void run() {
                SyncTask toProcess = toDoTask.poll();
                workOn(toProcess);
            }
        };

        timer = new Timer("MyTimer");//create a new Timer

        timer.scheduleAtFixedRate(timerTask, this.delay, this.interval);//this line starts the timer at the same time its executed
    }


    public void stop() {
        timer.cancel();
    }

}
