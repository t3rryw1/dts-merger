package com.cozystay.model;

public interface SyncTask extends Cloneable{

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

    class SyncOperation{
        
    }

    class SyncItem<T> {
        public String fieldName;
        public T originValue;
        public T currentValue;
    }

}
