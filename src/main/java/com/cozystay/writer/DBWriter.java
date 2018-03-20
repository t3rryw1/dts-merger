package com.cozystay.writer;

import com.aliyun.drc.client.message.DataMessage;

public interface DBWriter {
    void write(DataMessage.Record record);
}
