package com.cozystay.structure;

import com.cozystay.model.SyncTask;

public interface TaskRunner {

    void start();

    void workOn();

    void stop();

}
