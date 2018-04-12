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
        System.out.println("set up " + this.getClass().getName());
    }
    @Test
    public void testGetter(){
        SyncTask task = new SyncTaskImpl("id-123-abc","test-db","test-table");
        assertEquals(task.getOperations().size(),0);
        assertEquals(task.getDatabase(),"test-db");
        assertEquals(task.getTable(),"test-table");
        assertEquals(task.getId(),"test-db:test-table:id-123-abc");
    }

    @Test
    public void testMerge(){
        SyncTask task1 = new SyncTaskImpl("id-123-abc","test-db","test-table");
        SyncTask task2 = new SyncTaskImpl("id-123-abc","test-db","test-table");
        SyncOperation.SyncItem item = new SyncOperation.SyncItem<>("name", "aa", "bb");

        task1.addOperation(
                new SyncOperationImpl(null,
                        SyncOperation.OperationType.CREATE,
                        new ArrayList<>(Arrays.asList(item)),
                        "source1",
                        new ArrayList<>(Arrays.asList("source1", "source2")),
                        new Date())
        );

        task2.addOperation(
                new SyncOperationImpl(null,
                        SyncOperation.OperationType.CREATE,
                        new ArrayList<>(Arrays.asList(item)),
                        "source2",
                        new ArrayList<>(Arrays.asList("source1", "source2")),
                        new Date())
        );

        task1.getOperations().get(0).setSourceSend("source2");
        task1.merge(task2);

        Assert.assertTrue(task1.allOperationsCompleted());
    }

    @After
    public void tearDown(){
        System.out.println("tear down " + this.getClass().getName());
    }
}
