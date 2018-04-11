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
        SyncOperation.SyncItem item1 = new SyncOperation.SyncItem<>("user_notes", "val1", "val2");

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
        SyncOperation.SyncItem item1 = new SyncOperation.SyncItem<>("name", "aa", "bb");
        SyncOperation.SyncItem item2 = new SyncOperation.SyncItem<>("name", "aa", "xx");
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
        SyncOperation.SyncItem item1 = new SyncOperation.SyncItem<>("name", "aa", "bb");

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
        SyncOperation.SyncItem item1 = new SyncOperation.SyncItem<>("name", "aa", "bb");

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
    public void testSQL() {

    }


    @After
    public void tearDown() {
        System.out.println("tear down " + this.getClass().getName());
    }
}
