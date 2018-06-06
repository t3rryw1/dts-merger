package com.cozystay.notify;

import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;

import java.io.*;
import java.util.*;

import org.yaml.snakeyaml.Yaml;

public class HttpSyncNotifierImpl implements SyncNotifier {
    private List<NotifyRule> notifyRules;

    @Override
    public void loadRules() throws FileNotFoundException {
        Yaml yaml = new Yaml();
        File file = new File("src/main/resources/notify-rules.yaml");
        FileInputStream stream = new FileInputStream(file);

        List<NotifyRule> notifyRules = new ArrayList<>();
        List rules = yaml.loadAs(stream, List.class);

        for (Object rule : rules) {
            try {
                HashMap map = (HashMap) rule;
                String key = (String) map.get("key");
                String path = (String) map.get("path");
                String tableName = (String) map.get("tableName");
                NotifyRule.requestMethod method = NotifyRule.requestMethod.valueOf((String) map.get("method"));
                List<String> fieldNames = (List<String>) map.get("fieldNames");

                NotifyRule notifyRule = new NotifyRuleImpl(key,
                        path,
                        tableName,
                        method,
                        fieldNames);

                notifyRules.add(notifyRule);
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
        }

        this.notifyRules = notifyRules;
    }

    public List<NotifyRule> getNotifyRules() {
        return notifyRules;
    }

    @Override
    public boolean matchTask(SyncTask task) {
        SyncOperation operation = task.getOperations().get(0);
        for (NotifyRule rule : notifyRules) {
            if (rule.operationMatchedRule(operation)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void notify(SyncTask task) {
        SyncOperation operation = task.getOperations().get(0);
        List<NotifyRule.NotifyAction> actions = new ArrayList<>();

        for (NotifyRule rule : notifyRules) {
            if (rule.operationMatchedRule(operation)) {
                actions.add(rule.acceptOperation(operation));
            }
        }

        if (actions.size() > 0) {
            for (NotifyRule.NotifyAction action : actions) {
                try {
                    action.sendRequest();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
