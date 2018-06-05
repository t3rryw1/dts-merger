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

        SyncTask task = new SyncTaskImpl("id-123-abc","test-db","test-table", SyncOperation.OperationType.CREATE);
        SyncOperation.SyncItem item = new SyncOperation.SyncItem<>("title", "aa", "bb", SyncOperation.SyncItem.ColumnType.CHAR,true);

        task.addOperation(
                new SyncOperationImpl(null,
                        SyncOperation.OperationType.CREATE,
                        new ArrayList<>(Arrays.asList(item)),
                        "source1",
                        new ArrayList<>(Arrays.asList("source1", "source2")),
                        new Date().getTime())
        );

        Assert.assertTrue(notifier.matchTask(task));
    }

    @Test
    public void testNotifyAction() {

    }

    @After
    public void tearDown(){
        System.out.println("tear down " + this.getClass().getName());
    }
}
