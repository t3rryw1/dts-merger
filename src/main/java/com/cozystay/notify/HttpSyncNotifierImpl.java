package com.cozystay.notify;

import com.cozystay.model.SyncTask;

import java.util.Properties;

public class HttpSyncNotifierImpl implements SyncNotifier {
    @Override
    public void loadRules(Properties properties) {

    }

    @Override
    public boolean matchTask(SyncTask task) {
        return false;
    }

    @Override
    public boolean notify(SyncTask task) {
        return false;
    }
}
