package com.cozystay.db;

import com.cozystay.model.SyncTask;

public interface Writer {
    void write(SyncTask task);
}
