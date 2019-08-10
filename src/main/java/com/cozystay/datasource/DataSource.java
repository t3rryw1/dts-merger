package com.cozystay.datasource;

import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;

import java.sql.SQLException;

//loadRules from .properties, read Record from DTS and pass it to its callback
public interface DataSource {

    void consumeData(SyncTask task);

    int writeDB(SyncOperation operation) throws SQLException;

    void start();

    void stop();

    void init();

    boolean isRunning();

    String getName();
}