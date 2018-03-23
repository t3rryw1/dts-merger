package com.cozystay.dts;

import com.aliyun.drc.clusterclient.message.ClusterMessage;
import com.cozystay.model.SyncTask;

//load from .properties, read Record from DTS and pass it to its callback
public interface DataSource {

    boolean shouldConsume(SyncTask task);

    void consumeData(SyncTask task);

    boolean shouldWriteDB(SyncTask task);

    void writeDB(SyncTask task);

    void start();

    void stop();

    boolean shouldFilterMessage(ClusterMessage message);

}