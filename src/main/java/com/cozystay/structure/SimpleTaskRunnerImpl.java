package com.cozystay.structure;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class SimpleTaskRunnerImpl implements TaskRunner {
    private final int delay;
    private final int threadNumber;
    ThreadPoolExecutor executor;
    private Timer timer;

    protected SimpleTaskRunnerImpl(int delay, int threadNumber) {

        this.delay = delay;
        this.threadNumber = threadNumber;
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
        executor = new ThreadPoolExecutor(threadNumber,
                100,
                10000,
                TimeUnit.DAYS,
                queue);
    }

    public void start() {
        try {
            Thread.sleep(this.delay);
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    executor.execute(new Runnable() {
                        @Override
                        public void run() {
                            workOn();
                        }
                    });

                }
            };
            timer = new Timer(true);
            timer.scheduleAtFixedRate(task, 0, 10);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    public void stop() {
        executor.shutdown();
        timer.cancel();

    }

}
