package com.cozystay.model;


import org.junit.*;

import java.util.*;

public class TestSyncOperation {
    @Before
    public void setUp() {
        System.out.println("set up " + this.getClass().getName());
    }

    @Test
    public void testCreation(){
        SyncTask task = new SyncTaskImpl("id-123", "cozystay_db", "user");
        SyncOperation.SyncItem item1 = new SyncOperation.SyncItem<>("user_notes", "val1", "val2",SyncOperation.SyncItem.ColumnType.CHAR,true);

        SyncOperation operation = new SyncOperationImpl( task,
                SyncOperation.OperationType.CREATE,
                new ArrayList<>(Arrays.asList(item1)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2")),
                new Date());

        Assert.assertEquals(operation.getSyncItems().get(0).fieldName,"user_notes");
        Assert.assertEquals(operation.getSyncItems().get(0).originValue,"val1");
        Assert.assertEquals(operation.getSyncItems().get(0).currentValue,"val2");
        Assert.assertEquals(operation.getSyncStatus().get("source1"), SyncOperation.SyncStatus.COMPLETED);
        Assert.assertEquals(operation.getSyncStatus().get("source2"), SyncOperation.SyncStatus.INIT);
    }

    @Test
    public void testSameOperation(){
        SyncTask task = new SyncTaskImpl("id-123", "cozystay_db", "user");
        SyncOperation.SyncItem item1 = new SyncOperation.SyncItem<>("name", "aa", "bb",SyncOperation.SyncItem.ColumnType.CHAR,true);
        SyncOperation.SyncItem item2 = new SyncOperation.SyncItem<>("name", "aa", "xx",SyncOperation.SyncItem.ColumnType.CHAR,true);
        List<String> sources = new ArrayList<>(Arrays.asList("source1", "source2"));

        SyncOperation operation1 = new SyncOperationImpl( task,
                SyncOperation.OperationType.CREATE,
                new ArrayList<>(Arrays.asList(item1)),
                "source1",
                sources,
                new Date());

        SyncOperation operation2 = new SyncOperationImpl( task,
                SyncOperation.OperationType.CREATE,
                new ArrayList<>(Arrays.asList(item1)),
                "source1",
                sources,
                new Date());

        SyncOperation operation3 = new SyncOperationImpl( task,
                SyncOperation.OperationType.CREATE,
                new ArrayList<>(Arrays.asList(item2)),
                "source2",
                sources,
                new Date());

        Assert.assertTrue(operation1.isSameOperation(operation2));
        Assert.assertFalse(operation1.isSameOperation(operation3));
    }

    @Test
    public void testStatus(){
        SyncTask task = new SyncTaskImpl("id-123", "cozystay_db", "user");
        SyncOperation.SyncItem item1 = new SyncOperation.SyncItem<>("name", "aa", "bb",SyncOperation.SyncItem.ColumnType.CHAR,true);

        SyncOperation operation = new SyncOperationImpl( task,
                SyncOperation.OperationType.CREATE,
                new ArrayList<>(Arrays.asList(item1)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2")),
                new Date());

        operation.updateStatus("source1", SyncOperation.SyncStatus.COMPLETED);
        Assert.assertEquals(operation.getSyncStatus().get("source1"), SyncOperation.SyncStatus.COMPLETED);
    }

    @Test
    public void testMerge(){
        SyncTask task = new SyncTaskImpl("id-123", "cozystay_db", "user");
        SyncOperation.SyncItem item1 = new SyncOperation.SyncItem<>("name", "aa", "bb",SyncOperation.SyncItem.ColumnType.CHAR,true);

        SyncOperation operation = new SyncOperationImpl( task,
                SyncOperation.OperationType.CREATE,
                new ArrayList<>(Arrays.asList(item1)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2")),
                new Date());

        SyncOperation ToMergeOperation = new SyncOperationImpl( task,
                SyncOperation.OperationType.CREATE,
                new ArrayList<>(Arrays.asList(item1)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2")),
                new Date());

        ToMergeOperation.setSourceSend("source2");
        operation.mergeStatus(ToMergeOperation);
        Assert.assertEquals(operation.getSyncStatus().get("source1"), SyncOperation.SyncStatus.COMPLETED);
        Assert.assertEquals(operation.getSyncStatus().get("source2"), SyncOperation.SyncStatus.SEND);
    }


    @Test
    public void testSQLInSingleItem() {
        String tableName = "listing";
        String indexFieldName = "title";
        String originValue = "the string";
        String currentValue = "the newer string";

        SyncTask task = new SyncTaskImpl("id-111", "cozystay_db", tableName);
        SyncOperation.SyncItem itemWithIndex = new SyncOperation.SyncItem<>(indexFieldName, originValue, currentValue,SyncOperation.SyncItem.ColumnType.CHAR,true);

        SyncOperation operationCreate = new SyncOperationImpl( task,
                SyncOperation.OperationType.CREATE,
                new ArrayList<>(Arrays.asList(itemWithIndex)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2")),
                new Date());
        Assert.assertEquals(operationCreate.buildSql(), String.format("INSERT INTO %s (%s) VALUES ('%s');", tableName, indexFieldName, currentValue));

        SyncOperation operationDELETE = new SyncOperationImpl( task,
                SyncOperation.OperationType.DELETE,
                new ArrayList<>(Arrays.asList(itemWithIndex)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2")),
                new Date());
        Assert.assertEquals(operationDELETE.buildSql(), String.format("DELETE FROM %s WHERE  %s = '%s' ;", tableName, indexFieldName, originValue));

        SyncOperation operationREPLACE = new SyncOperationImpl( task,
                SyncOperation.OperationType.REPLACE,
                new ArrayList<>(Arrays.asList(itemWithIndex)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2")),
                new Date());
        Assert.assertEquals(operationREPLACE.buildSql(), String.format("UPDATE %s SET  %s = '%s'  WHERE  %s = '%s' ;", tableName, indexFieldName, currentValue, indexFieldName, originValue));


        SyncOperation operationUPDATE = new SyncOperationImpl( task,
                SyncOperation.OperationType.UPDATE,
                new ArrayList<>(Arrays.asList(itemWithIndex)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2")),
                new Date());
        Assert.assertEquals(operationUPDATE.buildSql(), String.format("UPDATE %s SET  %s = '%s'  WHERE  %s = '%s' ;", tableName, indexFieldName, currentValue, indexFieldName, originValue));
    }

    @Test
    public void testSQLInMultiItems() {
        String tableName = "listing";
        String indexFieldName1 = "title";
        String indexFieldName2 = "id";
        String fieldName1 = "description";
        String fieldName2 = "house_manuel";
        String originValue = "the string";
        String currentValue = "the newer string";

        SyncTask task = new SyncTaskImpl("id-111", "cozystay_db", tableName);

        SyncOperation.SyncItem itemWithIndex1 = new SyncOperation.SyncItem<>(indexFieldName1, originValue, currentValue,SyncOperation.SyncItem.ColumnType.CHAR,true);
        SyncOperation.SyncItem itemWithIndex2 = new SyncOperation.SyncItem<>(indexFieldName2, originValue, currentValue,SyncOperation.SyncItem.ColumnType.CHAR,true);

        SyncOperation.SyncItem itemWithoutIndex1 = new SyncOperation.SyncItem<>(fieldName1, originValue, currentValue, SyncOperation.SyncItem.ColumnType.CHAR,false);
        SyncOperation.SyncItem itemWithoutIndex2 = new SyncOperation.SyncItem<>(fieldName2, originValue, currentValue, SyncOperation.SyncItem.ColumnType.CHAR,false);

        SyncOperation operationCreateWithMultiItems = new SyncOperationImpl( task,
                SyncOperation.OperationType.CREATE,
                new ArrayList<>(Arrays.asList(itemWithIndex1, itemWithoutIndex1)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2")),
                new Date());
        Assert.assertEquals(operationCreateWithMultiItems.buildSql(), String.format("INSERT INTO %s (%s, %s) VALUES ('%s', '%s');", tableName, indexFieldName1, fieldName1, currentValue, currentValue));

        SyncOperation operationDeleteWithMultiItems = new SyncOperationImpl( task,
                SyncOperation.OperationType.DELETE,
                new ArrayList<>(Arrays.asList(itemWithIndex1, itemWithoutIndex1)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2")),
                new Date());
        Assert.assertEquals(operationDeleteWithMultiItems.buildSql(), String.format("DELETE FROM %s WHERE  %s = '%s' ;", tableName, indexFieldName1, originValue));

        SyncOperation operationReplaceWithMultiItems = new SyncOperationImpl( task,
                SyncOperation.OperationType.REPLACE,
                new ArrayList<>(Arrays.asList(itemWithIndex1, itemWithoutIndex1)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2")),
                new Date());
        System.out.println(operationReplaceWithMultiItems.buildSql());
        Assert.assertEquals(operationReplaceWithMultiItems.buildSql(), String.format("UPDATE %s SET  %s = '%s' ,  description = '%s'  WHERE  %s = '%s' ;", tableName, indexFieldName1, currentValue, currentValue, indexFieldName1, originValue));

        SyncOperation operationUpdateWithMultiItems = new SyncOperationImpl( task,
                SyncOperation.OperationType.UPDATE,
                new ArrayList<>(Arrays.asList(itemWithIndex1, itemWithoutIndex1)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2")),
                new Date());
        Assert.assertEquals(operationUpdateWithMultiItems.buildSql(), String.format("UPDATE %s SET  %s = '%s' ,  description = '%s'  WHERE  %s = '%s' ;", tableName, indexFieldName1, currentValue, currentValue, indexFieldName1, originValue));

        SyncOperation operationUpdateWithoutIndexItems = new SyncOperationImpl( task,
                SyncOperation.OperationType.UPDATE,
                new ArrayList<>(Arrays.asList(itemWithoutIndex1, itemWithoutIndex2)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2")),
                new Date());
        Assert.assertEquals(operationUpdateWithoutIndexItems.buildSql(), "");

        SyncOperation operationUpdateWithMultiIndexItems = new SyncOperationImpl( task,
                SyncOperation.OperationType.UPDATE,
                new ArrayList<>(Arrays.asList(itemWithIndex1, itemWithIndex2)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2")),
                new Date());
        Assert.assertEquals(operationUpdateWithMultiIndexItems.buildSql(), String.format("UPDATE %s SET  %s = '%s' ,  %s = '%s'  WHERE  %s = '%s'  and  %s = '%s' ;", tableName, indexFieldName1, currentValue, indexFieldName2, currentValue, indexFieldName1, originValue, indexFieldName2, originValue));
    }


    @After
    public void tearDown() {
        System.out.println("tear down " + this.getClass().getName());
    }
}
