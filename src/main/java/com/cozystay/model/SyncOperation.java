package com.cozystay.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.math.BigDecimal;
import java.util.*;

public interface SyncOperation {
    String toString();

    SyncTask getTask();

    Long getTime();

    OperationType getOperationType();

    List<SyncItem> getSyncItems();

    String getSource();

    Map<String, SyncStatus> getSyncStatus();

    String buildSql();

    boolean isSameOperation(SyncOperation operation);

    void updateStatus(String source, SyncStatus status);

    void reduceItems();

    boolean shouldSendToSource(String name);

    void setSourceSend(String name);

    void mergeStatus(SyncOperation toMergeOp);

    SyncOperation deepMerge(SyncOperation toMergeOp);

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

    class SyncItem<T > {
        public final String fieldName;
        public final T originValue;
        public final T currentValue;
        public final boolean isIndex;
        public final ColumnType fieldType;

        SyncItem() {

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
                    .append(fieldType, item.fieldType)
                    .append(isIndex, item.isIndex)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(fieldName)
                    .append(currentValue)
                    .append(fieldType)
                    .append(isIndex)
                    .toHashCode();
        }

        SyncItem mergeItem(SyncItem item) {
            return new SyncItem<>(
                    this.fieldName,
                    this.originValue,
                    item.currentValue,
                    this.fieldType,
                    this.isIndex
            );
        }

        String originValueToString() {
            if (originValue == null) {
                return null;
            }
            if (originValue instanceof Integer
                    ||
                    originValue instanceof Double
                    ||
                    originValue instanceof BigDecimal
                    ||
                    originValue instanceof Short
                    ||
                    originValue instanceof Long
                    ||
                    originValue instanceof Float
                    ) {
                return originValue
                        .toString();

            } else {
                return "'" + originValue
                        .toString()
                        .replaceAll("'","''")
                        .replaceAll("\\\\","\\\\\\\\")
                        + "'";
            }
        }

        String currentValueToString() {
            if (currentValue == null) {
                return null;
            }
            if (currentValue instanceof Integer
                    ||
                    currentValue instanceof Double
                    ||
                    currentValue instanceof BigDecimal
                    ||
                    currentValue instanceof Short
                    ||
                    currentValue instanceof Long
                    ||
                    currentValue instanceof Float
                    ) {
                return currentValue
                        .toString();

            } else {
                return "'" + currentValue
                        .toString()
                        .replaceAll("'","''")
                        .replaceAll("\\\\","\\\\\\\\")
                        + "'";

            }
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
