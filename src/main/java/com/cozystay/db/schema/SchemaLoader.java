package com.cozystay.db.schema;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaLoader {

    private final String dbAddress;
    private final Integer dbPort;
    private final String dbUser;
    private final String dbPassword;

    private final Map<String, SchemaDatabase> databaseMap;

    public SchemaLoader(String dbAddress, Integer dbPort, String dbUser, String dbPassword) {

        this.dbAddress = dbAddress;
        this.dbPort = dbPort;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.databaseMap = new HashMap<>();
    }


    public SchemaTable getTable(String dbName, String tableName){
        return this.databaseMap.get(dbName).getTable(tableName);
    }

    public void loadDBSchema(SchemaRuleCollection collection) {
        String connString = String.format("jdbc:mysql://%s:%d/",
                this.dbAddress,
                this.dbPort);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(connString, dbUser, dbPassword);
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet tableResultSet = metaData.getTables(null, "public", "%", new String[]{"TABLE"})) {
                while (tableResultSet.next()) {
                    String dbName = tableResultSet.getString(1);
                    SchemaDatabase database;
                    if (databaseMap.containsKey(dbName)) {
                        database = databaseMap.get(dbName);
                    } else {
                        database = new SchemaDatabase(dbName, "UTF-8");//TODO: find out encoding
                        databaseMap.put(dbName, database);
                    }
                    String tableName = tableResultSet.getString(3);
                    SchemaTable table = new SchemaTable(tableName, "UTF-8");
                    database.addTable(tableName, table);

                    List<String> indexFields = tableIndexFields(metaData,
                            collection,
                            dbName,
                            tableName);

                    try (ResultSet columnResultSet = metaData.getColumns(null, "public", tableName, "%")) {
                        int index = 1;
                        while (columnResultSet.next()) {
                            String columnName = columnResultSet.getString("COLUMN_NAME");
                            String columnType = columnResultSet.getString("DATA_TYPE");
                            String typeName = columnResultSet.getString("TYPE_NAME");
                            System.out.printf("%s %s %s %s %s %n", columnName, columnType, typeName, dbName, tableName);
                            if (indexFields.contains(columnName)) {
                                table.addField(new SchemaField(columnName, typeName, index, true));
                            } else {
                                table.addField(new SchemaField(columnName, typeName, index, false));
                            }
                            index++;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection != null && !connection.isClosed()) {
                    System.out.print("load completed");
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    private List<String> tableIndexFields(DatabaseMetaData metaData,
                                          SchemaRuleCollection ruleCollection,
                                          String dbName,
                                          String tableName) {

        List<String> presetFields = ruleCollection.getPrimaryKeys(dbName, tableName);
        if (presetFields != null) {
            return presetFields;
        }
        try {
            List<String> fieldsFromSchema = new ArrayList<>();
            ResultSet set = metaData.getPrimaryKeys(dbName, null, tableName);
            while (set.next()) {
                fieldsFromSchema.add(set.getString(4));
            }
            return fieldsFromSchema;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

    }

}
