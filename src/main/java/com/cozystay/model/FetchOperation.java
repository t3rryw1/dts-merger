package com.cozystay.model;

import java.sql.SQLException;
import java.util.List;

public interface FetchOperation {
    FetchOperation setDB(String dbName);

    FetchOperation setTable(String tableName);

    FetchOperation setConditions(List< String> conditionList)  throws IllegalArgumentException;

    FetchOperation addCondition(String key, String value, String operator);

    DataItemList fetchList() throws SQLException;

    DataItem fetchFirst();

}
