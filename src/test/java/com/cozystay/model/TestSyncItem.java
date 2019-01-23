package com.cozystay.model;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSyncItem {
    @Before
    public void setUp() {
        System.setProperty("COZ_MERGE_HOME", "..");
        System.out.println("set up " + this.getClass().getName());
    }

    @Test
    public void testEqual() {
        SyncItem<String> item1 = new SyncItem<>("abc", "value1", "value2", SyncItem.ColumnType.CHAR,true);
        SyncItem<String> item2 = new SyncItem<>("abc", "value1", "value2", SyncItem.ColumnType.CHAR,true);
        Assert.assertEquals(item1, item2);
        Assert.assertEquals(item1.hashCode(), item2.hashCode());

        SyncItem<Integer> item3 = new SyncItem<>("abc", 123, 456, SyncItem.ColumnType.CHAR,true);
        SyncItem<Integer> item4 = new SyncItem<>("abc", 123, 456, SyncItem.ColumnType.CHAR,true);
        Assert.assertEquals(item3, item4);
        Assert.assertEquals(item3.hashCode(), item4.hashCode());

    }

    @Test
    public void testMergeItem() {
        SyncItem<String> item1 = new SyncItem<>("abc", "value1", "value2", SyncItem.ColumnType.CHAR,true);
        SyncItem<String> item2 = new SyncItem<>("abc", "value1", "value2", SyncItem.ColumnType.CHAR,true);
        SyncItem mergedItem = item1.mergeItem(item2);
        Assert.assertEquals(mergedItem.currentValue, item2.currentValue);
    }

    @Test
    public void testEqualNull() {
        SyncItem<String> item1 = new SyncItem<>("abc", null, "value2", SyncItem.ColumnType.CHAR,true);
        SyncItem<String> item2 = new SyncItem<>("abc", null, "value2", SyncItem.ColumnType.CHAR,true);
        Assert.assertEquals(item1, item2);
        Assert.assertEquals(item1.hashCode(), item2.hashCode());
        SyncItem<String> item3 = new SyncItem<>("abc", null, null, SyncItem.ColumnType.CHAR,true);
        SyncItem<String> item4 = new SyncItem<>("abc", null, null, SyncItem.ColumnType.CHAR,true);
        SyncItem<String> item5 = new SyncItem<>("abc", "value1", "value2", SyncItem.ColumnType.CHAR,true);
        Assert.assertEquals(item3, item4);
        Assert.assertEquals(item3.hashCode(), item4.hashCode());
        Assert.assertEquals(item2, item5);
    }

    @Test
    public void testEscape(){
        SyncItem<String> item1 = new SyncItem<>("abc", null, "\\n\\naa", SyncItem.ColumnType.TEXT,true);
        Assert.assertEquals(item1.currentValueToString(),"'\\\\n\\\\naa'");

    }



    @After
    public void tearDown() {
        System.out.println("tear down " + this.getClass().getName());
    }
}
