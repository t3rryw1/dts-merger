package com.cozystay.model;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestSyncTask {

    @Before
    public void setUp(){
//        System.out.println("set up " + this.getClass().getName());
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
    public void testStatus(){

    }
    @Test
    public void testMerge(){

    }

    @After
    public void tearDown(){
//        System.out.println("tear down " + this.getClass().getName());
    }
}
