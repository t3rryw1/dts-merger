package com.cozystay.model;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;


public class TestSyncTask {

    @Before
    public void setUp(){
        System.setProperty("COZ_MERGE_HOME", "..");
        System.out.println("set up " + this.getClass().getName());
    }
    @Test
    public void testGetter(){
        SyncTask task = new SyncTaskImpl("id-123-abc","test-db","test-table", SyncOperation.OperationType.CREATE);
        assertEquals(task.getOperations().size(),0);
        assertEquals(task.getDatabase(),"test-db");
        assertEquals(task.getTable(),"test-table");
        assertEquals(task.getId(),"test-db:test-table:CREATE:id-123-abc");
    }

    @Test
    public void testMerge(){
        SyncTask task1 = new SyncTaskImpl("id-123-abc","test-db","test-table", SyncOperation.OperationType.CREATE);
        SyncTask task2 = new SyncTaskImpl("id-123-abc","test-db","test-table", SyncOperation.OperationType.CREATE);
        SyncItem item = new SyncItem<>("name", "aa", "bb", SyncItem.ColumnType.CHAR,true);

        task1.addOperation(
                new SyncOperationImpl(null,
                        SyncOperation.OperationType.CREATE,
                        new ArrayList<>(Arrays.asList(item)),
                        "source1",
                        new ArrayList<>(Arrays.asList("source1", "source2")),
                        new Date().getTime())
        );

        task2.addOperation(
                new SyncOperationImpl(null,
                        SyncOperation.OperationType.CREATE,
                        new ArrayList<>(Arrays.asList(item)),
                        "source2",
                        new ArrayList<>(Arrays.asList("source1", "source2")),
                        new Date().getTime())
        );

        task1.getOperations().get(0).setSourceSend("source2");
        task1.merge(task2);

        Assert.assertTrue(task1.allOperationsCompleted());
    }

    @Test
    public void testMergeStatus(){
        SyncTask task1 = new SyncTaskImpl("id-123-abc","test-db","test-table", SyncOperation.OperationType.UPDATE);
        SyncTask task2 = new SyncTaskImpl("id-123-abc","test-db","test-table", SyncOperation.OperationType.UPDATE);
        SyncItem item = new SyncItem<>("name", "aa", "bb", SyncItem.ColumnType.CHAR,true);
        task1.addOperation(new SyncOperationImpl(null,
                SyncOperation.OperationType.CREATE,
                new ArrayList<>(Arrays.asList(item)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2")),
                new Date().getTime()));
        task2.addOperation(new SyncOperationImpl(null,
                SyncOperation.OperationType.CREATE,
                new ArrayList<>(Arrays.asList(item)),
                "source2",
                new ArrayList<>(Arrays.asList("source1", "source2")),
                new Date().getTime()));

        task1.mergeStatus(task2);

        Assert.assertTrue(task1.allOperationsCompleted());
    }

    @Test
    public void testDeepMerge() throws InterruptedException {
        SyncTask task1 = new SyncTaskImpl("id-123-abc","test-db","test-table", SyncOperation.OperationType.UPDATE);
        SyncTask task2 = new SyncTaskImpl("id-123-abc","test-db","test-table", SyncOperation.OperationType.UPDATE);

        //set items
        SyncItem editName1 = new SyncItem<>(
                "name",
                "aa",
                "bb",
                SyncItem.ColumnType.CHAR,
                true);

        SyncItem editName2 = new SyncItem<>(
                "name",
                "aa",
                "cc",
                SyncItem.ColumnType.CHAR,
                true);

        SyncItem editGender1 = new SyncItem<>(
                "gender",
                "men",
                "women",
                SyncItem.ColumnType.CHAR,
                true);

        SyncItem editGender2 = new SyncItem<>(
                "gender",
                "men",
                "other",
                SyncItem.ColumnType.CHAR,
                true);


        SyncItem editOtherField1 = new SyncItem<>(
                "other1",
                "11",
                "22",
                SyncItem.ColumnType.CHAR,
                true);

        SyncItem editOtherField2 = new SyncItem<>(
                "other2",
                "22",
                "ssd",
                SyncItem.ColumnType.CHAR,
                true);


        //add operation
        task1.addOperation(new SyncOperationImpl(null,
                SyncOperation.OperationType.UPDATE,
                new ArrayList<>(Arrays.asList(editName1, editOtherField2)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2", "source3", "source4")),
                new Date().getTime()));

        Thread.sleep(50);

        task1.addOperation(new SyncOperationImpl(null,
                SyncOperation.OperationType.UPDATE,
                new ArrayList<>(Arrays.asList(editName1)),
                "source2",
                new ArrayList<>(Arrays.asList("source1", "source2", "source3", "source4")),
                new Date().getTime()));

        Thread.sleep(50);

        task1.addOperation(new SyncOperationImpl(null,
                SyncOperation.OperationType.UPDATE,
                new ArrayList<>(Arrays.asList(editGender1)),
                "source3",
                new ArrayList<>(Arrays.asList("source1", "source2", "source3", "source4")),
                new Date().getTime()));

        Thread.sleep(50);

        task2.addOperation(new SyncOperationImpl(null,
                SyncOperation.OperationType.UPDATE,
                new ArrayList<>(Arrays.asList(editName1)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2", "source3", "source4")),
                new Date().getTime()));

        Thread.sleep(50);

        task2.addOperation(new SyncOperationImpl(null,
                SyncOperation.OperationType.UPDATE,
                new ArrayList<>(Arrays.asList(editName2)),
                "source2",
                new ArrayList<>(Arrays.asList("source1", "source2", "source3", "source4")),
                new Date().getTime()));

        Thread.sleep(50);

        task2.addOperation(new SyncOperationImpl(null,
                SyncOperation.OperationType.UPDATE,
                new ArrayList<>(Arrays.asList(editGender2, editOtherField1)),
                "source4",
                new ArrayList<>(Arrays.asList("source1", "source2", "source3", "source4")),
                new Date().getTime()));

        SyncTask mergedTask = task1.deepMerge(task2);
    }


    @After
    public void tearDown(){
        System.out.println("tear down " + this.getClass().getName());
    }
}
