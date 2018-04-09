package com.cozystay.model;

import java.util.*;

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
        Iterator<Map.Entry<Object, Object>> it = prop.entrySet().iterator();
        FilterRuleList list = new FilterRuleList();
        list.rules = new LinkedList<>();
        while (it.hasNext()) {
            Map.Entry<Object, Object> entry = it.next();
            if (entry.getKey().toString().startsWith("filter.")) {
                String value = entry.getValue().toString().trim();
                list.rules.add(parseString(value));
            }
        }
        return list;

    }

    static  FilterRule parseString(String str) {
        String[] res = str.split("\\.");
        if (res.length != 3) {
            throw new IllegalArgumentException("Wrong filter format, must follow db.table.field format");
        }
        return new FilterRule(res[0], res[1], res[2]);
    }

    static class FilterRule {
        private final String databaseName;
        private final String tableName;
        private final String fieldName;

        public String getDatabaseName() {
            return databaseName;
        }

        public String getTableName() {
            return tableName;
        }

        public String getFieldName() {
            return fieldName;
        }

        FilterRule(String databaseName, String tableName, String fieldName) {
            if (databaseName.equals("*")) {
                this.databaseName = null;
            } else {
                this.databaseName = databaseName;

            }
            if (tableName.equals("*")) {
                this.tableName = null;
            } else {
                this.tableName = tableName;
            }

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
