package com.cozystay.model;

import com.cozystay.dts.DataSource;

public interface SyncTask {

    boolean hasDone(SyncTask newRecord);

    String getSource();

    void setSourceFinished(String source);

    boolean allSourcesFinished();

    boolean contains(SyncTask task);

    SyncTaskImpl merge(SyncTask task);

    String getId();

    String getDatabase();

    String buildSql();


    boolean shouldWriteSource(String source);

    void setSourceWritten(String name);

    enum SyncStatus{
        INITED,
        SEND,
        COMPLETED
    }

    class SyncItem<T> {
        public String fieldName;
        public T originValue;
        public T currentValue;
    }

}
