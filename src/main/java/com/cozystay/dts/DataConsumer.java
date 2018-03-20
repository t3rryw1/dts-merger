package com.cozystay.dts;

import com.aliyun.drc.client.message.DataMessage;

//load from .properties, read Record from DTS and pass it to its callback
public interface DataConsumer {
    void consumeData(DataMessage.Record record);

    void start();

    boolean shouldConsume(DataMessage.Record record);
}