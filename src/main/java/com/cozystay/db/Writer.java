package com.cozystay.db;

import com.cozystay.model.SyncOperation;

import java.sql.SQLException;

public interface Writer {
    boolean write(SyncOperation operation) throws SQLException;
}
