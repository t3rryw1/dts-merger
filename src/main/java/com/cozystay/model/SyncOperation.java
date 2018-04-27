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
        public final ColumnType fieldType;

        SyncItem(){

            fieldName = null;

            originValue = null;
            currentValue = null;

            isIndex = false;
            fieldType = null;
        }

        public SyncItem(String fieldName,
                        T originValue,
                        T currentValue,
                        ColumnType columnType,
                        boolean isIndex) {
            this.fieldName = fieldName;
            this.originValue = originValue;
            this.currentValue = currentValue;
            this.isIndex = isIndex;
            this.fieldType = columnType;
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
                    .append(fieldType, item.fieldType)
                    .append(isIndex, item.isIndex)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(fieldName)
                    .append(currentValue)
                    .append(originValue)
                    .append(fieldType)
                    .append(isIndex)
                    .toHashCode();
        }


        public boolean hasChange() {
            if (this.originValue == null && this.currentValue == null)
                return false;
            if (this.originValue == null) {
                return true;
            }
            if (this.currentValue == null) {
                return true;
            }

            return !this.originValue.equals(this.currentValue);
        }


        public enum ColumnType {
            BIT,
            BOOL,
            BOOLEAN,

            TINYTEXT,
            MEDIUMTEXT,
            LONGTEXT,
            TEXT,

            ENUM,


            INT,
            UNSIGNED_INT,
            TINYINT,
            UNSIGNED_TINYINT,
            SMALLINT,
            UNSIGNED_SMALLINT,
            MEDIUMINT,
            UNSIGNED_MEDIUMINT,
            BIGINT,
            UNSIGNED_BIGINT,

            DOUBLE,
            UNSIGNED_DOUBLE,
            DECIMAL,
            UNSIGNED_DECIMAL,

            DATE,
            TIMESTAMP,
            DATETIME,
            TIME,
            YEAR,

            VARCHAR,
            CHAR,
            JSON;


            public static ColumnType fromString(String type) {
                try {
                    return ColumnType.valueOf(type);
                } catch (IllegalArgumentException e) {
                    switch (type) {
                        case "INT UNSIGNED":
                            return UNSIGNED_INT;
                        case "TINYINT UNSIGNED":
                            return UNSIGNED_TINYINT;
                        case "SMALLINT UNSIGNED":
                            return UNSIGNED_SMALLINT;
                        case "MEDIUMINT UNSIGNED":
                            return UNSIGNED_MEDIUMINT;
                        case "BIGINT UNSIGNED":
                            return UNSIGNED_BIGINT;
                        case "DECIMAL UNSIGNED":
                            return UNSIGNED_DECIMAL;
                        case "DOUBLE UNSIGNED":
                            return UNSIGNED_DOUBLE;
                        default:
                            return null;

                    }

                }
            }
        }
    }


}
