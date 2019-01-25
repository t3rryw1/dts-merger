package com.cozystay.model;

public interface DataItem {
    String getUpdateSql( String tableName);

    String getIndex();

    DataItem merge(DataItem item1);

    void setUpdateFlag(boolean b);
}
