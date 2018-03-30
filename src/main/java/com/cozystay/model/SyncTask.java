package com.cozystay.model;

import java.util.List;

public interface SyncTask extends Cloneable {

    boolean completeAllOperations();

    SyncTask merge(SyncTask task);

    String getId();

    String getDatabase();

    String getTable();

    List<SyncOperation> getOperations();


}
