package com.cozystay.model;

import com.healthmarketscience.sqlbuilder.BinaryCondition;
import com.healthmarketscience.sqlbuilder.InsertQuery;
import com.healthmarketscience.sqlbuilder.UpdateQuery;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSchema;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSpec;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbTable;
import javafx.util.Pair;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataItemImpl implements DataItem {
    private final String orderKey;
    private final Object orderValue;
    private HashMap<String, Object> objectMap;
    private boolean conflict = false;
    private List<Pair<String, Object>> keyValueList;


    DataItemImpl(Map<String, Object> objectMap, String orderKey, List<String> keyList) {
        this.objectMap = new HashMap<>(objectMap);
        keyValueList = keyList.stream()
                .map(key -> new Pair<>(key, objectMap.get(key)))
                .collect(Collectors.toList());
        this.orderKey = orderKey;
        this.orderValue = objectMap.get(orderKey);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DataItemImpl)) {
            return false;
        }
        DataItemImpl anotherObj = (DataItemImpl) obj;
        return objectMap.equals(anotherObj.objectMap);
    }

    @Override
    public String getUpdateSql(String tableName) {
        if (this.conflict) {
            DbSpec spec = new DbSpec();
            DbSchema schema = spec.addDefaultSchema();
            DbTable customerTable = schema.addTable(tableName);
            UpdateQuery query = new UpdateQuery(tableName);
            keyValueList.forEach((keyValue) -> {
                DbColumn columnName;
                if (keyValue.getValue() instanceof String) {
                    columnName = customerTable.addColumn(keyValue.getKey(), Types.VARCHAR, 50);
                } else {
                    columnName = customerTable.addColumn(keyValue.getKey(), Types.INTEGER, 10);

                }
                query.addCondition(BinaryCondition.equalTo(columnName, keyValue.getValue()));
            });

            objectMap.forEach((key, value) -> {
                        if (value instanceof String) {
                            query.addCustomSetClause(String.format("`%s`", key),
                                    mysqlRealScapeString(value.toString()));
                        } else {
                            query.addCustomSetClause(String.format("`%s`", key),
                                    value);
                        }
                    }
            );

            return query.toString();

        } else {
            InsertQuery query = new InsertQuery(tableName);
            objectMap.forEach((key, value) -> {
                if (value instanceof String) {
                    query.addCustomColumn(String.format("`%s`", key),
                            mysqlRealScapeString(value.toString()));
                } else {
                    query.addCustomColumn(String.format("`%s`", key),
                            value);
                }
            });
            return query.toString();

        }

    }

    private String mysqlRealScapeString(String str) {
        String data = null;
        if (str != null && str.length() > 0) {
            str = str.replace("\\", "\\\\");
            str = str.replace("'", "\\'");
            str = str.replace("\0", "\\0");
            str = str.replace("\n", "\\n");
            str = str.replace("\r", "\\r");
            str = str.replace("\"", "\\\"");
            str = str.replace("\\x1a", "\\Z");
            data = str;
        }
        return data;
    }

    @Override
    public String getIndex() {
        return keyValueList.stream()
                .map(k -> k.getValue() != null ? k.getValue().toString() : "")
                .collect(Collectors.joining(":"));

    }

    @Override
    public void merge(DataItem item2) {
        if (!(item2 instanceof DataItemImpl)) {
            return;
        }
        DataItemImpl item1 = (DataItemImpl) item2;
        if (this.orderValue.toString().compareTo(item1.orderValue.toString()) > 0) {
            return;
        } else {
            this.objectMap = (HashMap<String, Object>) item1.objectMap.clone();
        }

    }

    @Override
    public void setUpdateFlag(boolean b) {
        this.conflict = b;
    }

    @Override
    public String toString() {
        return String.format("index:%s,\t%s:%s\tdata:%s,",
                getIndex(),
                this.orderKey,
                this.orderValue.toString(),
                this.objectMap.toString());
    }
}
