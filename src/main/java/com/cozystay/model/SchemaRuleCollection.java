package com.cozystay.model;

import java.text.MessageFormat;
import java.util.*;

public class SchemaRuleCollection {
    List<FilterRule> filterRules;
    private List<IndexRule> indexRules;

    private SchemaRuleCollection() {
        filterRules = new ArrayList<>();
        indexRules = new ArrayList<>();
    }

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
        String[] res = value.split("\\.");
        if (res.length != 3) {
            throw new IllegalArgumentException(MessageFormat.format("Wrong filter format, {0} not following db.table.field format", value));
        }
        String[] fields = res[2].split(",");
        List<String> indicesFields = Arrays.asList(fields);
        return new IndexRule(res[0], res[1], indicesFields);
    }


    static FilterRule parseFilter(String str) {
        String[] res = str.split("\\.");
        if (res.length != 3) {
            throw new IllegalArgumentException(MessageFormat.format("Wrong filter format, {0} not following db.table.field format", str));
        }
        return new FilterRule(res[0], res[1], res[2]);
    }

    public List<String> getPrimaryKeys(String dbName, String tableName) {

        for (IndexRule rule : indexRules) {
            if (rule.isTargetTable(tableName, dbName)) {
                return rule.getIndexFields();
            }
        }
        return null;

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

    private static class IndexRule {
        private final String databaseName;
        private final String tableName;
        private final List<String> indexFields;

        List<String> getIndexFields() {
            return indexFields;
        }

        IndexRule(String databaseName, String tableName, List<String> indexFields) {
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

        boolean isTargetTable(String tableName, String dbName) {
            if (this.databaseName == null) {
                return this.tableName.equals(tableName);
            }
            return this.databaseName.equals(dbName) && this.tableName.equals(tableName);

        }


    }
}
