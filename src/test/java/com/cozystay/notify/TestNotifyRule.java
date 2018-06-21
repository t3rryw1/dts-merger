package com.cozystay.notify;

import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncOperationImpl;
import com.cozystay.model.SyncTask;
import com.cozystay.model.SyncTaskImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.*;


public class TestNotifyRule {
    @Before
    public void setUp() {
        System.setProperty("COZ_MERGE_HOME", "..");
        System.out.println("set up " + this.getClass().getName());
    }

    @Test
    public void testLoadRules() throws FileNotFoundException {
        SyncNotifier notifier = new HttpSyncNotifierImpl();
        notifier.loadRules();

        Assert.assertEquals(notifier.getNotifyRules().size(), 2);
    }

    @Test
    public void testMatchTask() throws FileNotFoundException {
        SyncNotifier notifier = new HttpSyncNotifierImpl();
        notifier.loadRules();

        SyncTask task = new SyncTaskImpl("id-123-abc","eadu","listing", SyncOperation.OperationType.CREATE);
        SyncOperation.SyncItem item = new SyncOperation.SyncItem<>("title", "aa", "121212", SyncOperation.SyncItem.ColumnType.CHAR,true);
        SyncOperation.SyncItem item1 = new SyncOperation.SyncItem<>("listing_id", "aa", "sdahsdj", SyncOperation.SyncItem.ColumnType.CHAR,true);

        task.addOperation(
                new SyncOperationImpl(null,
                        SyncOperation.OperationType.CREATE,
                        new ArrayList<>(Arrays.asList(item, item1)),
                        "source1",
                        new ArrayList<>(Arrays.asList("source1", "source2")),
                        new Date().getTime())
        );

        Assert.assertTrue(notifier.matchTask(task));

        SyncTask task1 = new SyncTaskImpl("id-123-abc","eadu","listing", SyncOperation.OperationType.CREATE);
        SyncOperation.SyncItem item2 = new SyncOperation.SyncItem<>("name", "aa", "bb", SyncOperation.SyncItem.ColumnType.CHAR,true);

        task1.addOperation(
                new SyncOperationImpl(null,
                        SyncOperation.OperationType.CREATE,
                        new ArrayList<>(Arrays.asList(item2)),
                        "source1",
                        new ArrayList<>(Arrays.asList("source1", "source2")),
                        new Date().getTime())
        );

        Assert.assertFalse(notifier.matchTask(task1));

        SyncTask task2 = new SyncTaskImpl("id-123-abc","test1","listing", SyncOperation.OperationType.CREATE);

        task1.addOperation(
                new SyncOperationImpl(null,
                        SyncOperation.OperationType.CREATE,
                        new ArrayList<>(Arrays.asList(item, item1)),
                        "source1",
                        new ArrayList<>(Arrays.asList("source1", "source2")),
                        new Date().getTime())
        );

        Assert.assertFalse(notifier.matchTask(task2));
    }

    @Test
    public void testNotifyAction() throws FileNotFoundException {
        SyncNotifier notifier = new HttpSyncNotifierImpl();
        notifier.loadRules();

        SyncTask task = new SyncTaskImpl("id-123-abc","eadu","listing", SyncOperation.OperationType.CREATE);
        SyncOperation.SyncItem item = new SyncOperation.SyncItem<>("title", "aa", "121212", SyncOperation.SyncItem.ColumnType.CHAR,true);
        SyncOperation.SyncItem item1 = new SyncOperation.SyncItem<>("listing_id", "aa", "sdahsdj", SyncOperation.SyncItem.ColumnType.CHAR,true);

        task.addOperation(
                new SyncOperationImpl(null,
                        SyncOperation.OperationType.CREATE,
                        new ArrayList<>(Arrays.asList(item,item1)),
                        "source1",
                        new ArrayList<>(Arrays.asList("source1", "source2")),
                        new Date().getTime())
        );

        notifier.notify(task);
    }

    @After
    public void tearDown(){
        System.out.println("tear down " + this.getClass().getName());
    }
}
