package com.cozystay.model;

import com.healthmarketscience.sqlbuilder.BinaryCondition;
import com.healthmarketscience.sqlbuilder.InsertQuery;
import com.healthmarketscience.sqlbuilder.UpdateQuery;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSchema;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSpec;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbTable;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class DataItemImpl implements DataItem {
    private final Object id;
    private final Object updateTime;
    private HashMap<String, Object> objectMap;
    private boolean conflict = false;


    DataItemImpl(Map<String, Object> objectMap, String orderKey) {
        this.objectMap = new HashMap<>(objectMap);
        this.id = objectMap.get("id");
        this.updateTime = objectMap.get(orderKey);

    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DataItemImpl)) {
            return false;
        }
        DataItemImpl anotherObj = (DataItemImpl) obj;
        return objectMap.equals(anotherObj.objectMap);
    }

    @Override
    public String getUpdateSql(String tableName) {
        if (this.conflict) {
            DbSpec spec = new DbSpec();
            DbSchema schema = spec.addDefaultSchema();
            DbTable customerTable = schema.addTable(tableName);
            if(id instanceof String){
                customerTable.addColumn("id", Types.VARCHAR,50);
            }else{
                customerTable.addColumn("id", Types.INTEGER,10);

            }
            DbColumn columnName = customerTable.findColumn("id");

            UpdateQuery query = new UpdateQuery(tableName);
            for (Map.Entry<String, Object> item : objectMap.entrySet()) {
                query.addCustomSetClause(item.getKey(), item.getValue());
            }
            query.addCondition(BinaryCondition.equalTo(columnName, id));
            return query.toString();

        } else {
            InsertQuery query = new InsertQuery(tableName);
            for (Map.Entry<String, Object> item : objectMap.entrySet()) {
                query.addCustomColumn(item.getKey(), item.getValue());
            }

            return query.toString();

        }

    }

    @Override
    public Object getId() {
        return id;
    }

    @Override
    public void merge(DataItem item2) {
        if (!(item2 instanceof DataItemImpl)) {
            return;
        }
        DataItemImpl item1 = (DataItemImpl) item2;
        if (this.updateTime.toString().compareTo(item1.updateTime.toString()) > 0) {
            return;
        } else {
            this.objectMap = (HashMap<String, Object>) item1.objectMap.clone();
        }

    }

    @Override
    public void setUpdateFlag(boolean b) {
        this.conflict = b;
    }
}
