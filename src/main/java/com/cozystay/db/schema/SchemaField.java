package com.cozystay.db.schema;

import com.cozystay.model.SyncOperation;

public class SchemaField {
    public final String columnName;
    public final SyncOperation.SyncItem.ColumnType columnType;
    final int index;
    public final boolean isPrimary;

    public SchemaField(String columnName, String columnType, int index, boolean isPrimary) {
        this.columnName = columnName;
        this.columnType = SyncOperation.SyncItem.ColumnType.fromString(columnType);
        this.index = index;
        this.isPrimary = isPrimary;
    }
}
