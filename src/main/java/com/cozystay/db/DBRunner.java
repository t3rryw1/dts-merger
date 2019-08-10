package com.cozystay.db;

import com.cozystay.model.SyncOperation;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface DBRunner {
    int write(SyncOperation operation) throws SQLException;

    List<Map<String,Object>> query(String dbName, String queryString) throws SQLException;

    boolean update(String dbName, String queryString);

    String getDBInfo();

}
