package com.cozystay;

import com.aliyun.drc.client.message.DataMessage;
import com.cozystay.dts.AbstractDataConsumer;
import com.cozystay.dts.DataConsumer;
import com.cozystay.queue.TwoWayDBConnectQueue;
import com.cozystay.queue.WorkerQueue;
import com.cozystay.queue.WorkerQueueImpl;

public class Runner {
    public static void main(String[] args) throws Exception {
        System.out.print("Runner");
        final WorkerQueue queue = new WorkerQueueImpl() {

            @Override
            public void start() {

            }

            @Override
            public void stop() {

            }

            @Override
            protected void work(DataMessage.Record toProcess) {

            }
        };

        DataConsumer consumer1 = new AbstractDataConsumer("","","") {
            @Override
            public void consumeData(DataMessage.Record record) {
                queue.addTask(record);

            }

            @Override
            public boolean shouldConsume(DataMessage.Record record) {
                return false;
            }
        };

        consumer1.start();

        DataConsumer consumer2 = new AbstractDataConsumer("","","") {
            @Override
            public void consumeData(DataMessage.Record record) {
                queue.addTask(record);

            }

            @Override
            public boolean shouldConsume(DataMessage.Record record) {
                return false;
            }
        };
        consumer2.start();

        queue.start();
    }
}
