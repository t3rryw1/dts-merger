package com.cozystay.structure;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class SimpleTaskRunnerImpl implements TaskRunner {
    private final int delay;
    private final int threadNumber;
    private boolean stop = false;
    ThreadPoolExecutor executor;

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
            while (!stop) {
                Thread.sleep(100);
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        workOn();
                    }
                });
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    public void stop() {
        stop = true;
        executor.shutdown();

    }

}
