package com.cozystay.notify;

import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import net.sf.json.JSONObject;

public class HttpSyncNotifierImpl implements SyncNotifier {
    private static Logger logger = LoggerFactory.getLogger(SyncNotifier.class);
    private List<NotifyRule> notifyRules;

    @Override
    public void loadRules() {
        Yaml yaml = new Yaml();
        InputStream stream = HttpSyncNotifierImpl.class.getResourceAsStream("/notify-rules.yaml");

        List<NotifyRule> notifyRules = new ArrayList<>();
        List rules = yaml.loadAs(stream, List.class);

        for (Object rule : rules)
            try {
                HashMap map = (HashMap) rule;
                Boolean matchAllDatabase = (Boolean) map.get("matchAllDatabase");
                Boolean matchAllTable = (Boolean) map.get("matchAllTable");
                Boolean matchAllField = (Boolean) map.get("matchAllField");
                String database = matchAllDatabase ? null : (String) map.get("database");
                String tableName = matchAllTable ? null : (String) map.get("tableName");
                List<String> fieldNames = matchAllField ? null : (List<String>) map.get("fieldNames");
                NotifyRule.requestMethod method = NotifyRule.requestMethod.valueOf((String) map.get("method"));
                String path = (String) map.get("path");

                List<String> keyFieldNames = new ArrayList<>();
                Pattern pattern = Pattern.compile("\\$\\{(.*?)}");
                Matcher matcher = pattern.matcher(path);
                while (matcher.find()) {
                    keyFieldNames.add(matcher.group(1));
                }

                NotifyRule notifyRule = new NotifyRuleImpl(
                        matchAllDatabase,
                        matchAllTable,
                        matchAllField,
                        database,
                        tableName,
                        fieldNames,
                        keyFieldNames,
                        method,
                        path
                );

                notifyRules.add(notifyRule);
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

        this.notifyRules = notifyRules;
    }

    public List<NotifyRule> getNotifyRules() {
        return notifyRules;
    }

    @Override
    public boolean matchTask(SyncTask task) {
        if (task.getOperations().size() < 1){
            return false;
        }
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
        List<NotifyAction> actions = new ArrayList<>();

        for (NotifyRule rule : notifyRules) {
            if (rule.operationMatchedRule(operation)) {
                actions.add(rule.acceptOperation(operation));
            }
        }

        if (actions.size() > 0) {
            for (NotifyAction action : actions) {
                try {
                    JSONObject resJson = action.sendRequest();
                    if (!resJson.get("code").equals(0)) {
                        logger.error("notify action failed: {}", resJson.toString());
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
