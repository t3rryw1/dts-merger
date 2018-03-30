package com.cozystay.model;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;
import java.util.Map;

public interface SyncOperation {

    SyncTask getTask();

    OperationType getOperationType();

    List<SyncItem> getSyncItems();

    Map<String, SyncStatus> getSyncStatus();

    String buildSql();

    boolean isSameOperation(SyncOperation operation);

    void updateStatus(String source, SyncStatus status);

    boolean shouldSendToSource(String name);

    void setSourceSend(String name);

    void merge(SyncOperation toMergeOp);

    boolean completedAllSources();

    void setTask(SyncTask syncTask);

    boolean collideWith(SyncOperation selfOp);

    SyncOperation resolveCollide(SyncOperation toMergeOp);


    enum SyncStatus {
        INITED,
        SEND,
        COMPLETED
    }

    enum OperationType {
        CREATE,
        UPDATE,
        REPLACE,
        DELETE
    }

    class SyncItem<T> {
        public final String fieldName;
        public final T originValue;
        public final T currentValue;

        public SyncItem(String fieldName, T originValue, T currentValue) {
            this.fieldName = fieldName;
            this.originValue = originValue;
            this.currentValue = currentValue;
        }


        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof SyncItem))
                return false;
            SyncItem item = (SyncItem) obj;
            return item.fieldName.equals(fieldName)
                    &&
                    item.currentValue.equals(currentValue)
                    &&
                    item.originValue.equals(originValue);
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(fieldName)
                    .append(currentValue)
                    .append(originValue)
                    .toHashCode();
//            return Objects.hashCode(fieldName, currentValue, originValue);
        }
    }


}
