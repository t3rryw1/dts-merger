package com.cozystay.model;

import javafx.util.Pair;

import java.util.List;

public interface FetchOperation {
    FetchOperation setDB(String dbName);

    FetchOperation setTable(String tableName);

    FetchOperation setCondition(List<Pair<String, Pair<String, String>>> conditionList);

    FetchOperation addCondition(String key, String value, String operator);

    DataItemList fetchList();

    DataItem fetchFirst();

}
