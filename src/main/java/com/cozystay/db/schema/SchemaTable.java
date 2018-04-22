package com.cozystay.db.schema;

import java.util.ArrayList;
import java.util.List;

public class SchemaTable {
    final String tableName;
    final String tableEncoding;
    final List<SchemaField> fieldList;

    public SchemaTable(String tableName, String tableEncoding) {
        this.tableName = tableName;

        this.tableEncoding = tableEncoding;
        this.fieldList = new ArrayList<>();
    }

    void addField(SchemaField field) {
        this.fieldList.add(field);
    }

    public List<SchemaField> getFieldList() {
        return fieldList;
    }

    public String getTableEncoding() {
        return tableEncoding;
    }

    public String getTableName() {
        return tableName;
    }
}
