package com.cozystay.model;

import com.cozystay.db.DBRunner;
import com.healthmarketscience.sqlbuilder.BinaryCondition;
import com.healthmarketscience.sqlbuilder.InCondition;
import com.healthmarketscience.sqlbuilder.OrderObject;
import com.healthmarketscience.sqlbuilder.SelectQuery;
import com.healthmarketscience.sqlbuilder.custom.mysql.MysLimitClause;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSchema;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSpec;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbTable;
import javafx.util.Pair;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FetchOperationImpl implements FetchOperation {

    private final DBRunner dbRunner;
    private final String orderKey;
    private final boolean silent;
    private String dbName;
    private String tableName;
    private List<Pair<String, Pair<String, String>>> conditionList = new LinkedList<>();
    private List<String> keyList;

    public FetchOperationImpl(DBRunner dbRunner, String orderKey, boolean silent, List<String> keyList) {

        this.dbRunner = dbRunner;
        this.orderKey = orderKey;
        this.silent = silent;
        this.keyList = keyList;
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
    public FetchOperation setConditions(List<String> conditionList) throws IllegalArgumentException {
        for (String conditionStr : conditionList) {
            String[] singleConditionArray = conditionStr.split("\\|");
            if (singleConditionArray.length == 2) {
                this.addCondition(singleConditionArray[0], singleConditionArray[1], "=");
            } else if (singleConditionArray.length == 3) {
                this.addCondition(singleConditionArray[0], singleConditionArray[2], singleConditionArray[1]);
            } else {
                throw new IllegalArgumentException("Illegal format for condition: " + conditionStr);
            }
        }
        return this;
    }

    @Override
    public FetchOperation addCondition(String key, String value, String operator) {
        this.conditionList.add(new Pair<>(key, new Pair<>(value, operator)));
        return this;
    }

    @Override
    public DataItemList fetchList() throws SQLException {
        List<Map<String, Object>> resData = dbRunner.query(this.dbName, this.formSql(null));

        System.out.format("[Info] Query return %d entries:\n", resData.size());

        return new DataItemListImpl(resData.stream().map(objectMap -> {
            DataItem dataItem = new DataItemImpl(objectMap, orderKey, keyList);
            if (!silent) {
                System.out.format("[Info] %s:\n", dataItem.toString());
            }
            return dataItem;
        }).toArray(DataItem[]::new));
    }

    @Override
    public DataItem fetchFirst() {
        try {
            List<Map<String, Object>> resData = dbRunner.query(this.dbName, this.formSql(1));
            return new DataItemImpl(resData.get(0), orderKey, keyList);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    private String formSql(Integer limit) {
        DbSpec spec = new DbSpec();
        DbSchema schema = spec.addDefaultSchema();
        DbTable customerTable = schema.addTable('`' + tableName + '`');
        DbColumn updateColumn = customerTable.addColumn('`' + this.orderKey + '`');
        SelectQuery query = new SelectQuery();
        query.addAllTableColumns(customerTable);
        conditionList.forEach(entry -> {
            String operand = entry.getValue().getValue().trim();
            DbColumn columnName = customerTable.addColumn('`' + entry.getKey() + '`');

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
                case "in":
                    List<String> idList = Arrays.asList(entry.getValue().getKey().split(","));
                    List<String> trimmed = idList.stream()
                            .map(String::trim).collect(Collectors.toList());
                    query.addCondition(new InCondition(columnName, trimmed));
                    break;
            }
        });

        query.addOrdering(updateColumn, OrderObject.Dir.DESCENDING);
        if (limit != null) {
            query.addCustomization(new MysLimitClause(limit));
        }
        return query.toString();

    }
}
