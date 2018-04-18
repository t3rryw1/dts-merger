package com.cozystay.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Date;
import java.util.List;
import java.util.Map;

public interface SyncOperation {

    String toString();

    SyncTask getTask();

    Date getTime();

    OperationType getOperationType();

    List<SyncItem> getSyncItems();

    Map<String, SyncStatus> getSyncStatus();

    String buildSql();

    boolean isSameOperation(SyncOperation operation);

    void updateStatus(String source, SyncStatus status);

    boolean shouldSendToSource(String name);

    void setSourceSend(String name);

    void mergeStatus(SyncOperation toMergeOp);

    boolean allSourcesCompleted();

    void setTask(SyncTask syncTask);

    boolean collideWith(SyncOperation selfOp);

    SyncOperation resolveCollide(SyncOperation toMergeOp);


    enum SyncStatus {
        INIT,
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
        public final boolean isIndex;

        public SyncItem(String fieldName, T originValue, T currentValue, boolean isIndex) {
            this.fieldName = fieldName;
            this.originValue = originValue;
            this.currentValue = currentValue;
            this.isIndex = isIndex;
        }


        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof SyncItem))
                return false;
            SyncItem item = (SyncItem) obj;
            return new EqualsBuilder()
                    .append(fieldName, item.fieldName)
                    .append(currentValue, item.currentValue)
                    .append(originValue, item.originValue)
                    .append(isIndex, item.isIndex)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(fieldName)
                    .append(currentValue)
                    .append(originValue)
                    .append(isIndex)
                    .toHashCode();
        }
    }


}
