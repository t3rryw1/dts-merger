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

    public boolean operationMatchedRule(SyncOperation operation) {
        for (SyncOperation.SyncItem item : operation.getSyncItems()) {
            if(itemMatchedRule(item)){
                return true;
            }
        }
        return false;
    }

    private Boolean itemMatchedRule(SyncOperation.SyncItem item){
        assert fieldList != null;
        return fieldList.contains(item.fieldName);
    }

    private NotifyAction getAction(List<SyncOperation.SyncItem> items, SyncOperation.SyncItem KeyItem) {
        String requestId = (String) KeyItem.currentValue;
        assert requestPath != null;
        assert requestId != null;
        String fullUrl = requestPath.replace("${id}", requestId);
        Map<String, String> params = new HashMap<>();
        Map<String, String> body =  new HashMap<>();
        assert requestMethod != null;
        switch (requestMethod) {
            case DELETE:
                return new NotifyAction(fullUrl,
                        requestMethod,
                        params,
                        body
                );

            case GET:
                items.forEach(e -> {
                    params.put(e.fieldName, (String) e.currentValue);
                });
                return new NotifyAction(fullUrl,
                    requestMethod,
                    params,
                    body
            );
            case PUT:
            case POST:
                items.forEach(e -> {
                    body.put(e.fieldName, (String) e.currentValue);
                });
                return new NotifyAction(fullUrl,
                        requestMethod,
                        params,
                        body
                );
        }

        return null;
    }


    public NotifyAction checkOperation(SyncOperation operation){
        List<SyncOperation.SyncItem> matchedKeyItems = operation
                .getSyncItems()
                .stream()
                .filter(e -> {
                    assert e.fieldName != null;
                    return e.fieldName.equals(requestKey);
                })
                .distinct()
                .collect(Collectors.toList());


        List<SyncOperation.SyncItem> matchedItems = operation
                .getSyncItems()
                .stream()
                .filter(this::itemMatchedRule)
                .distinct()
                .collect(Collectors.toList());

        if (matchedKeyItems.size() == 1 && matchedItems.size() > 0) {
            return getAction(matchedItems, matchedKeyItems.get(0));
        } else {
            return null;
        }
    }
}
