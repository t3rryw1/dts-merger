package com.cozystay.structure;

import java.util.Timer;
import java.util.TimerTask;

public abstract class SimpleTaskRunnerImpl implements TaskRunner {
    private final int delay;
    private final int interval;
    private Timer timer;


    protected SimpleTaskRunnerImpl(int delay, int interval) {

        this.delay = delay;
        this.interval = interval;
    }

    public void start() {
        TimerTask timerTask = new TimerTask() {

            @Override
            public void run() {
                workOn();
            }
        };

        timer = new Timer("MyTimer");//create a new Timer

        timer.scheduleAtFixedRate(timerTask, this.delay, this.interval);//this line starts the timer at the same time its executed
    }


    public void stop() {
        timer.cancel();
    }

}
