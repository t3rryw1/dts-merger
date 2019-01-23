package com.cozystay.db.schema;

import com.cozystay.model.SyncItem;
import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;

import java.text.MessageFormat;
import java.util.*;

public class SchemaRuleCollection {
    List<FilterRule> filterRules;
    private List<IndexRule> indexRules;
    private List<Map.Entry<String, String>> filterTableRule;
    private List<FilterRule> blackListRules;

    private SchemaRuleCollection() {
        filterRules = new ArrayList<>();
        indexRules = new ArrayList<>();
        filterTableRule = new ArrayList<>();
        blackListRules = new ArrayList<>();
    }

    public synchronized boolean filter(SyncOperation operation) {

        List<SyncItem> items = operation.getSyncItems();

        nextItem:
        for (SyncItem item : items) {
            if (!item.hasChange()) {
                continue;
            }
            for (FilterRule rule : filterRules) {
                if (rule.match(item, operation.getTask())) {
                    continue nextItem;
                }
            }
            return false;
        }
        return true;
    }

    public synchronized void removeBlacklisted(SyncOperation operation) {

        List<SyncItem> items = operation.getSyncItems();

        Iterator<SyncItem> i = items.iterator();
        while (i.hasNext()) {
            SyncItem item = i.next(); // must be called before you can call i.remove()
            for (FilterRule rule : blackListRules) {
                if (rule.match(item, operation.getTask())) {
                    i.remove();
                }
            }

            // Do something
        }
        return;
    }

    public synchronized static SchemaRuleCollection loadRules(Properties prop) {
        Iterator<Map.Entry<Object, Object>> it = prop.entrySet().iterator();
        SchemaRuleCollection list = new SchemaRuleCollection();
        while (it.hasNext()) {
            Map.Entry<Object, Object> entry = it.next();
            if (entry.getKey().toString().startsWith("schema.filter.")) {
                String value = entry.getValue().toString().trim();
                list.filterRules.add(parseFieldFilter(value));
            } else if (entry.getKey().toString().startsWith("schema.index.")) {
                String value = entry.getValue().toString().trim();
                list.indexRules.add(parseIndices(value));
            } else if (entry.getKey().toString().startsWith("schema.table_filter.")) {
                String value = entry.getValue().toString().trim();
                list.filterTableRule.add(parseTableFilter(value));
            } else if (entry.getKey().toString().startsWith("schema.field_blacklist.")) {
                String value = entry.getValue().toString().trim();
                list.blackListRules.add(parseFieldFilter(value));
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

    private static Map.Entry<String, String> parseTableFilter(String value) {
        String[] res = value.split("\\.");
        if (res.length != 2) {
            throw new IllegalArgumentException(MessageFormat.format("Wrong filter format, {0} not following db.table.field format", value));
        }
        return new AbstractMap.SimpleEntry<String, String>(res[0], res[1]);
    }


    static FilterRule parseFieldFilter(String str) {
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

    boolean isFilteredDB(String dbName) {
        for (Map.Entry<String, String> entry : filterTableRule) {
            if (!entry.getKey().equals(dbName)) {
                continue;
            }
            if (entry.getValue().equals("*")) {
                return true;
            }
        }

        return false;
    }

    boolean isFilteredTable(String dbName, String tableName) {
        for (Map.Entry<String, String> entry : filterTableRule) {
            if ((entry.getKey().equals(dbName)
                    ||
                    entry.getKey().equals("*"))
                    &&
                    entry.getValue().equals(tableName)) {
                return true;
            }
        }
        return false;

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


        boolean match(SyncItem item, SyncTask parentTask) {
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
