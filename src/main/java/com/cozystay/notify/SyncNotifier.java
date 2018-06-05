package com.cozystay.notify;

import com.cozystay.model.SyncTask;

import java.io.FileNotFoundException;
import java.util.List;

public interface SyncNotifier {

    void loadRules() throws FileNotFoundException;

    public List<NotifyRule> getNotifyRules();


    boolean matchTask(SyncTask task);

    boolean notify(SyncTask task);

}
