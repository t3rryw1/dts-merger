package com.cozystay.model;

import java.util.*;

public class SchemaRuleCollection {
    List<FilterRule> filterRules;
    List<IndexRule> indexRules;

    public boolean filter(SyncOperation operation) {

        List<SyncOperation.SyncItem> items = operation.getSyncItems();

        nextItem:
        for (SyncOperation.SyncItem item : items) {
            for (FilterRule rule : filterRules) {
                if (rule.match(item, operation.getTask())) {
                    continue nextItem;
                }
            }
            return false;
        }
        return true;
    }

    public static SchemaRuleCollection loadRules(Properties prop) {
        Iterator<Map.Entry<Object, Object>> it = prop.entrySet().iterator();
        SchemaRuleCollection list = new SchemaRuleCollection();
        list.filterRules = new LinkedList<>();
        while (it.hasNext()) {
            Map.Entry<Object, Object> entry = it.next();
            if (entry.getKey().toString().startsWith("schema.filter.")) {
                String value = entry.getValue().toString().trim();
                list.filterRules.add(parseFilter(value));
            } else if (entry.getKey().toString().startsWith("schema.index.")) {
                String value = entry.getValue().toString().trim();
                list.indexRules.add(parseIndices(value));
            }

        }
        return list;

    }

    private static IndexRule parseIndices(String value) {
        //TODO: impl IndexRule creations
        return null;
    }


    static FilterRule parseFilter(String str) {
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

    private class IndexRule {
        private final String databaseName;
        private final String tableName;
        private final List<String> indexFields;

        public String getDatabaseName() {
            return databaseName;
        }

        public String getTableName() {
            return tableName;
        }

        public List<String> getIndexFields() {
            return indexFields;
        }

        private IndexRule(String databaseName, String tableName, List<String> indexFields) {
            if (databaseName.equals("*")) {
                this.databaseName = null;
            } else {
                this.databaseName = databaseName;

            }
            this.tableName = tableName;
            if (this.tableName == null && this.databaseName == null) {
                throw new IllegalArgumentException("Index rule can not apply to every table");
            }
            this.indexFields = indexFields;
        }

        public boolean isTargetTable(String tableName, String dbName) {
            if (this.databaseName == null) {
                return this.tableName.equals(tableName);
            }
            return this.databaseName.equals(dbName) && this.tableName.equals(tableName);

        }


    }
}
