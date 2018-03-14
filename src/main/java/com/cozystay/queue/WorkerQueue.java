package com.cozystay.queue;

import com.aliyun.drc.client.message.DataMessage;

public interface WorkerQueue {
    public void addTask(DataMessage.Record newRecord);

    public void start();

    public void stop();

}
