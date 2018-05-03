package com.cozystay.db;

import com.cozystay.model.SyncOperation;

public interface Writer {
    boolean write(SyncOperation operation);
}
