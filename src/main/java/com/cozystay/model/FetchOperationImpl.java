package com.cozystay.model;

import com.cozystay.db.DBRunner;
import com.healthmarketscience.sqlbuilder.BinaryCondition;
import com.healthmarketscience.sqlbuilder.OrderObject;
import com.healthmarketscience.sqlbuilder.SelectQuery;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSchema;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSpec;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbTable;
import javafx.util.Pair;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class FetchOperationImpl implements FetchOperation {

    private final DBRunner dbRunner;
    private final String orderKey;
    private final String orderKeyType;
    private String dbName;
    private String tableName;
    private List<Pair<String, Pair<String, String>>> conditionList = new LinkedList<>();

    public FetchOperationImpl(DBRunner dbRunner, String orderKey, String orderKeyType) {

        this.dbRunner = dbRunner;
        this.orderKey = orderKey;
        this.orderKeyType = orderKeyType;
    }

    @Override
    public FetchOperation setDB(String dbName) {
        this.dbName = dbName;
        return this;
    }

    @Override
    public FetchOperation setTable(String tableName) {
        this.tableName = tableName;
        return this;
    }

    @Override
    public FetchOperation setCondition(List<Pair<String, Pair<String, String>>> conditionList) {
        this.conditionList = conditionList;
        return this;
    }

    @Override
    public FetchOperation addCondition(String key, String value, String operator) {
        this.conditionList.add(new Pair<>(key, new Pair<>(value, operator)));
        return this;
    }

    @Override
    public DataItemList fetchList() {
        try {
            List<Map<String, Object>> resData = dbRunner.query(this.dbName, this.formSql());
            DataItemList list = new DataItemListImpl();
            for (Map<String, Object> objectMap : resData) {
                DataItem dataItem = new DataItemImpl(objectMap, orderKey);
                list.add(dataItem);
            }

            return list;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public DataItem fetchFirst() {
        return null;
    }

    private String formSql() {
        DbSpec spec = new DbSpec();
        DbSchema schema = spec.addDefaultSchema();
        DbTable customerTable = schema.addTable(tableName);
        DbColumn updateColumn = customerTable.addColumn(this.orderKey, orderKeyType, null);
        SelectQuery query = new SelectQuery();
        query.addAllTableColumns(customerTable);
        for (Pair<String, Pair<String, String>> entry : conditionList) {
            String operand = entry.getValue().getValue().trim();
            DbColumn columnName = customerTable.findColumn(entry.getKey());
            switch (operand) {
                case "=":
                    query.addCondition(BinaryCondition.equalTo(columnName, entry.getValue().getKey()));
                    break;
                case ">":
                    query.addCondition(BinaryCondition.greaterThan(columnName, entry.getValue().getKey()));
                    break;
                case "<":
                    query.addCondition(BinaryCondition.lessThan(columnName, entry.getValue().getKey()));
                    break;
                case ">=":
                    query.addCondition(BinaryCondition.greaterThanOrEq(columnName, entry.getValue().getKey()));
                    break;
                case "<=":
                    query.addCondition(BinaryCondition.lessThanOrEq(columnName, entry.getValue().getKey()));
                    break;
                case "like":
                    query.addCondition(BinaryCondition.like(columnName, entry.getValue().getKey()));
                    break;
                case "<>":
                    query.addCondition(BinaryCondition.notEqualTo(columnName, entry.getValue().getKey()));
                    break;
            }
        }
        query.addOrdering(updateColumn, OrderObject.Dir.DESCENDING);
        return query.toString();

    }
}
