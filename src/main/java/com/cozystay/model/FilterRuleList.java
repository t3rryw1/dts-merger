package com.cozystay.model;

import java.util.List;
import java.util.Properties;

public class FilterRuleList {
    List<FilterRule> rules;

    public boolean filter(SyncOperation operation) {

        List<SyncOperation.SyncItem> items = operation.getSyncItems();

        nextItem:
        for (SyncOperation.SyncItem item : items) {
            for (FilterRule rule : rules) {
                if (rule.match(item, operation.getTask())) {
                    continue nextItem;
                }
            }
            return false;
        }
        return true;
    }

    public static FilterRuleList load(Properties prop) {
        return null;
    }

    class FilterRule {
        private final String databaseName;
        private final String tableName;
        private final String fieldName;

        FilterRule(String databaseName, String tableName, String fieldName) {
            this.databaseName = databaseName;
            this.tableName = tableName;
            this.fieldName = fieldName;
        }


        boolean match(SyncOperation.SyncItem item, SyncTask parentTask) {
            if (!fieldName.equals(item.fieldName)) {
                return false;
            }
            boolean dbMatch = databaseName == null || databaseName.equals(parentTask.getDatabase());
            boolean tableMatch = tableName == null || tableName.equals(parentTask.getTable());
            return dbMatch && tableMatch;
        }

    }
}
