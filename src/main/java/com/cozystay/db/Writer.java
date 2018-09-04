package com.cozystay.db;

import com.cozystay.model.SyncOperation;

import java.sql.SQLException;

public interface Writer {
    int write(SyncOperation operation) throws SQLException;
}
