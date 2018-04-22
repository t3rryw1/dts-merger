package com.cozystay.db.schema;

import java.util.HashMap;
import java.util.Map;

public class SchemaDatabase {
    final String dbName;
    final String dbEncoding;
    final Map<String, SchemaTable> tableList;

    public SchemaDatabase(String dbName, String dbEncoding) {
        this.dbName = dbName;
        this.dbEncoding = dbEncoding;
        tableList = new HashMap<>();

    }

    void addTable(String tableName, SchemaTable table) {
        this.tableList.put(tableName, table);
    }

    SchemaTable getTable(String tableName) {
        return this.tableList.get(tableName);
    }

}
