package com.cozystay.notify;

import com.cozystay.model.SyncOperation;

import java.util.Map;

public interface NotifyRule {

    enum requestMethod {
        POST,
        DELETE,
        PUT,
        GET
    }

    boolean operationMatchedRule(SyncOperation operation);

    NotifyAction checkOperation(SyncOperation operation);

    class NotifyAction {
        public final String requestUrl;
        public final requestMethod requestMethod;
        public final Map<String, String> requestParams;
        public final Map<String, String> requestBody;

        NotifyAction() {
            requestUrl = null;
            requestMethod = null;
            requestParams = null;
            requestBody = null;
        }

        public NotifyAction (String url,
                             requestMethod method,
                             Map<String, String> params,
                             Map<String, String> body
        ) {
            this.requestUrl = url;
            this.requestMethod = method;
            this.requestParams = params;
            this.requestBody = body;
        }

        public void run() {
            //start request

        }
    }
}
