package com.cozystay.model;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSyncItem {
    @Before
    public void setUp() {
        System.out.println("set up " + this.getClass().getName());
    }

    @Test
    public void testEqual(){
        SyncOperation.SyncItem item1 = new SyncOperation.SyncItem("abc","value1","value2");
        SyncOperation.SyncItem item2 = new SyncOperation.SyncItem("abc","value1","value2");
        Assert.assertEquals(item1,item2);
    }

    @Test
    public void testEqualNull(){
        SyncOperation.SyncItem<String> item1 = new SyncOperation.SyncItem<>("abc",null,"value2");
        SyncOperation.SyncItem<String> item2 = new SyncOperation.SyncItem("abc",null,"value2");
        Assert.assertEquals(item1,item2);
        SyncOperation.SyncItem<String> item3 = new SyncOperation.SyncItem<>("abc",null,null);
        SyncOperation.SyncItem<String> item4 = new SyncOperation.SyncItem("abc",null,null);
        Assert.assertEquals(item3,item4);
    }

    @After
    public void tearDown(){
//        System.out.println("tear down " + this.getClass().getName());
    }
}
