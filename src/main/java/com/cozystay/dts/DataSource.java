package com.cozystay.dts;

import com.aliyun.drc.clusterclient.message.ClusterMessage;
import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;

//loadRules from .properties, read Record from DTS and pass it to its callback
public interface DataSource {

    void consumeData(SyncTask task);

    void writeDB(SyncOperation operation);

    void start();

    void stop();

    boolean shouldFilterMessage(ClusterMessage message);

    String getName();
}