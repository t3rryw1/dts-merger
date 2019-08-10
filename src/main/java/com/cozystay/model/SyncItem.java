package com.cozystay.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.math.BigDecimal;

public class SyncItem<T > {
    public final String fieldName;
    final T originValue;
    final T currentValue;
    public final boolean isIndex;
    private final ColumnType fieldType;

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
        return getValueString(originValue);
    }

    private String getValueString(T originValue) {
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
        return getValueString(currentValue);
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
