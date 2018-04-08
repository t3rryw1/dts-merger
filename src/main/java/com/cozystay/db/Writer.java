package com.cozystay.db;

import com.cozystay.model.SyncOperation;

public interface Writer {
    void write(SyncOperation operation);
}
