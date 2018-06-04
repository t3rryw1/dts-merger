package com.cozystay.notify;

import com.cozystay.model.SyncTask;

import java.util.Properties;

public interface SyncNotifier {

    void loadRules(Properties properties);

    boolean matchTask(SyncTask task);

    boolean notify(SyncTask task);

}
