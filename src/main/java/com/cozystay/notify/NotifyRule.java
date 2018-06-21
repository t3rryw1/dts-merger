package com.cozystay.notify;

import com.cozystay.model.SyncOperation;

public interface NotifyRule {

    enum requestMethod {
        POST,
        DELETE,
        PUT,
        GET
    }

    boolean operationMatchedRule(SyncOperation operation);

    NotifyAction acceptOperation(SyncOperation operation);
}
