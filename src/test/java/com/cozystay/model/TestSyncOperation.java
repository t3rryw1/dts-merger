package com.cozystay.model;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class TestSyncOperation {
    @Before
    public void setUp() {
        System.out.println("set up " + this.getClass().getName());
    }

    private SyncOperation buildOperationWithOneItems(String dbName,
                                                     String tableName,
                                                     String fieldName1) {
        SyncOperation.SyncItem item1 = new SyncOperation.SyncItem<>(fieldName1, "val1", "val2");
        List<SyncOperation.SyncItem> items = new ArrayList<>(Collections.singletonList(item1));
        SyncTask task = new SyncTaskImpl("id-123", dbName, tableName);
        List<String> sources = new ArrayList<>(Arrays.asList("source1", "source2"));

        return new SyncOperationImpl(task,
                SyncOperation.OperationType.UPDATE,
                items,
                "source1",
                sources,
                new Date());

    }

    @Test
    public void testCreation() {
        SyncOperation operation = buildOperationWithOneItems("galaxy_eadu",
                "calendar",
                "user_notes");
        Assert.assertEquals(operation.getSyncItems().get(0).fieldName,"user_notes");
        Assert.assertEquals(operation.getSyncItems().get(0).originValue,"val1");
        Assert.assertEquals(operation.getSyncItems().get(0).currentValue,"val2");
        Assert.assertEquals(operation.getSyncStatus().get("source1"), SyncOperation.SyncStatus.COMPLETED);
        Assert.assertEquals(operation.getSyncStatus().get("source2"), SyncOperation.SyncStatus.INIT);
    }

    @Test
    public void testSameOperation() {

    }

    @Test
    public void testStatus() {

    }

    @Test
    public void testMerge() {

    }


    @Test
    public void testSQL() {

    }


    @After
    public void tearDown() {
        System.out.println("tear down " + this.getClass().getName());
    }
}
