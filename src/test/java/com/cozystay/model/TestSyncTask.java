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
        SyncOperation.SyncItem item = new SyncOperation.SyncItem<>("name", "aa", "bb", SyncOperation.SyncItem.ColumnType.CHAR,true);

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
        SyncOperation.SyncItem item = new SyncOperation.SyncItem<>("name", "aa", "bb", SyncOperation.SyncItem.ColumnType.CHAR,true);
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
        SyncTask task1 = new SyncTaskImpl("id-123-abc","test-db","test-table");
        SyncTask task2 = new SyncTaskImpl("id-123-abc","test-db","test-table");
        SyncOperation.SyncItem item1 = new SyncOperation.SyncItem<>("name", "aa", "bb", SyncOperation.SyncItem.ColumnType.CHAR, true);
        SyncOperation.SyncItem item2 = new SyncOperation.SyncItem<>("name", "cc", "dd", SyncOperation.SyncItem.ColumnType.CHAR,true);
        SyncOperation.SyncItem item3 = new SyncOperation.SyncItem<>("xxxx", "aa", "bb", SyncOperation.SyncItem.ColumnType.CHAR,false);
        SyncOperation.SyncItem item4 = new SyncOperation.SyncItem<>("xxxx", "cc", "dd", SyncOperation.SyncItem.ColumnType.CHAR,false);
        task1.addOperation(new SyncOperationImpl(null,
                SyncOperation.OperationType.CREATE,
                new ArrayList<>(Arrays.asList(item1, item3)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2")),
                new Date().getTime()));

        Thread.sleep(50);

        task2.addOperation(new SyncOperationImpl(null,
                SyncOperation.OperationType.CREATE,
                new ArrayList<>(Arrays.asList(item2, item4)),
                "source1",
                new ArrayList<>(Arrays.asList("source1", "source2")),
                new Date().getTime()));

        task1.deepMerge(task2);

        List<SyncOperation.SyncItem> itemList = task1.getOperations().get(0).getSyncItems();

        Assert.assertEquals(itemList.get(0).currentValue, "dd");
        Assert.assertEquals(itemList.get(1).currentValue, "dd");
    }

    @Test
    public void testMultipleTask(){
        SyncTask task1 = new SyncTaskImpl(
                "uuid",
                "database",
                "user_profile"
        );
        SyncOperation.SyncItem itemEditName1 = new SyncOperation.SyncItem<>(
                "nick_name",
                "artour",
                "lanaya",
                SyncOperation.SyncItem.ColumnType.CHAR,
                true
        );
        SyncOperation.SyncItem itemEditFirstName1 = new SyncOperation.SyncItem<>(
                "first_name",
                "tang",
                "mag",
                SyncOperation.SyncItem.ColumnType.CHAR,
                false
        );
        SyncOperation.SyncItem itemEditLastName1 = new SyncOperation.SyncItem<>(
                "last_name",
                "jing min",
                "artour",
                SyncOperation.SyncItem.ColumnType.CHAR,
                false
        );
        task1.addOperation(new SyncOperationImpl(
                null,
                SyncOperation.OperationType.UPDATE,
                new ArrayList<>(Arrays.asList(itemEditName1, itemEditFirstName1, itemEditLastName1)),
                ".com",
                new ArrayList<>(Arrays.asList(".com", ".cn")),
                new Date().getTime()
        ));

        
    }


    @After
    public void tearDown(){
        System.out.println("tear down " + this.getClass().getName());
    }
}
