package com.cozystay.notify;

import com.cozystay.model.SyncOperation;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class NotifyRuleImpl implements NotifyRule {
    private final Boolean matchAllDatabase;
    private final Boolean matchAllTable;
    private final Boolean matchAllField;
    private final String database;
    private final String tableName;
    private final List<String> fieldNames;
    private final List<String> keyFieldNames;
    private final requestMethod method;
    private final String path;

    NotifyRuleImpl() {
        matchAllDatabase = false;
        matchAllTable = false;
        matchAllField = false;
        database = null;
        tableName = null;
        fieldNames = null;
        keyFieldNames = null;
        method = null;
        path = null;
    }

    public NotifyRuleImpl(
            Boolean matchAllDatabase,
            Boolean matchAllTable,
            Boolean matchAllField,
            String database,
            String tableName,
            List<String> fieldNames,
            List<String> keyFieldNames,
            requestMethod method,
            String path
    ) {
        this.matchAllDatabase = matchAllDatabase;
        this.matchAllTable = matchAllTable;
        this.matchAllField = matchAllField;
        this.database = database;
        this.tableName = tableName;
        this.fieldNames = fieldNames;
        this.keyFieldNames = keyFieldNames;
        this.method = method;
        this.path = path;
    }

    private Boolean operationMatchedDatabase(SyncOperation operation) {
        if (matchAllDatabase) {
            return true;
        }
        return operation.getTask().getDatabase().equals(database);
    }

    private Boolean operationMatchedTable(SyncOperation operation) {
        if (matchAllTable) {
            return true;
        }
        return operation.getTask().getTable().equals(tableName);
    }

    private Boolean operationMatchedKey(SyncOperation operation) {
        if (matchAllField) {
            return true;
        }
        return operation
                .getSyncItems()
                .stream()
                .map(item -> item.fieldName)
                .distinct()
                .collect(Collectors.toList())
                .containsAll(keyFieldNames);
    }

    private Boolean operationMatchedItem(SyncOperation operation) {
        for (SyncOperation.SyncItem item : operation.getSyncItems()) {
            if(fieldNames.contains(item.fieldName)){
                return true;
            }
        }
        return false;
    }

    public boolean operationMatchedRule(SyncOperation operation) {
        return operationMatchedDatabase(operation)
                &&
                operationMatchedTable(operation)
                &&
                operationMatchedKey(operation)
                &&
                operationMatchedItem(operation);
    }

    private List<SyncOperation.SyncItem> operationGetKeyItems(SyncOperation operation) {
        return operation
                .getSyncItems()
                .stream()
                .filter(item -> keyFieldNames.contains(item.fieldName))
                .distinct()
                .collect(Collectors.toList());
    }

    private List<SyncOperation.SyncItem> operationGetItems(SyncOperation operation) {
        return operation
                .getSyncItems()
                .stream()
                .filter(item -> {
                    if (matchAllField) return true;
                    return fieldNames.contains(item.fieldName);
                })
                .distinct()
                .collect(Collectors.toList());
    }

    private NotifyAction getAction(List<SyncOperation.SyncItem> keyItems, List<SyncOperation.SyncItem> items) {
        String fullUrl = path;
        for (SyncOperation.SyncItem keyItem : keyItems) {
            String regex = "\\$\\{" + keyItem.fieldName + "}";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(fullUrl);
            fullUrl = matcher.replaceAll((String) keyItem.currentValue);
        }

        Map<String, String> params = new HashMap<>();
        Map<String, String> body =  new HashMap<>();
        items.forEach(e -> params.put(e.fieldName, (String) e.currentValue));
        items.forEach(e -> body.put(e.fieldName, (String) e.currentValue));

        return new NotifyActionImpl(fullUrl,
                method,
                params,
                body);
    }


    public NotifyAction acceptOperation(SyncOperation operation) {
        return getAction(
                operationGetKeyItems(operation),
                operationGetItems(operation)
        );
    }
}
