package com.cozystay.notify;
import com.cozystay.model.SyncOperation;

import java.util.*;
import java.util.stream.Collectors;


public class NotifyRuleImpl implements NotifyRule {
    private final String requestKey;
    private final String requestPath;
    private final String tableName;
    private final requestMethod requestMethod;
    private final List<String> fieldList;

    NotifyRuleImpl() {
        requestKey = null;
        requestPath = null;
        tableName = null;
        requestMethod = null;
        fieldList = null;
    }

    public NotifyRuleImpl(String key,
                          String path,
                          String tableName,
                          requestMethod method,
                          List<String> fieldList
    ) {
        this.requestKey = key;
        this.requestPath = path;
        this.tableName = tableName;
        this.requestMethod = method;
        this.fieldList = fieldList;
    }

    private Boolean operationMatchedTable(SyncOperation operation) {
        return operation.getTask().getTable().equals(tableName);
    }

    private Boolean operationMatchedKey(SyncOperation operation) {
        for (SyncOperation.SyncItem item : operation.getSyncItems()) {
            if(item.fieldName.equals(requestKey)){
                return true;
            }
        }
        return false;
    }

    private Boolean operationMatchedItem(SyncOperation operation) {
        for (SyncOperation.SyncItem item : operation.getSyncItems()) {
            if(fieldList.contains(item.fieldName)){
                return true;
            }
        }
        return false;
    }

    public boolean operationMatchedRule(SyncOperation operation) {
        return operationMatchedTable(operation) && operationMatchedKey(operation) && operationMatchedItem(operation);
    }

    private String operationGetKey(SyncOperation operation) {
        if (!operationMatchedKey(operation)) {
            return null;
        }

        return (String) operation.getSyncItems()
                .stream()
                .filter(item -> {
                    return item.fieldName.equals(requestKey);
                })
                .distinct()
                .collect(Collectors.toList()).get(0).currentValue;
    }

    private List<SyncOperation.SyncItem> operationGetItems(SyncOperation operation) {
        return operation
                .getSyncItems()
                .stream()
                .filter(item -> {
                    return fieldList.contains(item.fieldName);
                })
                .distinct()
                .collect(Collectors.toList());
    }

    private NotifyAction getAction(String requestId, List<SyncOperation.SyncItem> items) {
        String fullUrl = requestPath.replace("${id}", requestId);
        Map<String, String> params = new HashMap<>();
        Map<String, String> body =  new HashMap<>();
        items.forEach(e -> params.put(e.fieldName, (String) e.currentValue));
        items.forEach(e -> body.put(e.fieldName, (String) e.currentValue));

        return new NotifyAction(fullUrl,
                requestMethod,
                params,
                body);
    }


    public NotifyAction acceptOperation(SyncOperation operation){
        return getAction(operationGetKey(
                operation),
                operationGetItems(operation)
        );
    }
}
