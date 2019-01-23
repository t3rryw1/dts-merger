package com.cozystay.model;

public interface DataItem {
    String getUpdateSql( String tableName);

    Object getId();

    void merge(DataItem item1);

    void setUpdateFlag(boolean b);
}
