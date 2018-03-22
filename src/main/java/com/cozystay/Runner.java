package com.cozystay;

import com.cozystay.dts.AbstractDataSourceImpl;
import com.cozystay.dts.DataSource;
import com.cozystay.model.SyncTask;
import com.cozystay.structure.ProcessedTaskPool;
import com.cozystay.structure.SimpleProcessedTaskPool;
import com.cozystay.structure.SimpleWorkerQueueImpl;
import com.cozystay.structure.WorkerQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Runner {
    public static int MAX_DATABASE_SIZE = 10;

    public static void main(String[] args) throws Exception {
        System.out.print("DB Sync runner launched");
        final List<DataSource> dataSources = new ArrayList<>();
        final ProcessedTaskPool taskPool = new SimpleProcessedTaskPool() {
            @Override
            public boolean hasDuplicateTask(SyncTask task) {
                return false;
            }

            @Override
            public SyncTask merge(SyncTask task) {
                return null;
            }
        };


        final WorkerQueue queue = new SimpleWorkerQueueImpl(1, 20) {
            @Override
            public void workOn(SyncTask task) {
                boolean processed =false;
                for (DataSource source :
                        dataSources) {
                    if (source.shouldWriteDB(task)) {
                        source.writeDB(task);
                        processed =true;
                    }
                }
                if(!processed){
                    taskPool.remove(task.getId());
                }

            }
        };
        queue.start();


        Properties prop = new Properties();
        prop.load(Runner.class.getResourceAsStream("/db-config.properties"));

        for (int i = 1; i <= MAX_DATABASE_SIZE; i++) {
            try {

                final DataSource source = new AbstractDataSourceImpl(prop,"db"+i) {
                    @Override
                    public void consumeData(SyncTask task) {
                        queue.addTask(task);

                    }

                    @Override
                    public boolean shouldConsume(SyncTask task) {
                        return !taskPool.hasDuplicateTask(task);
                    }

                    @Override
                    public boolean shouldWriteDB(SyncTask task) {
                        if (!super.shouldWriteDB(task)) {
                            return false;
                        }
                        if (taskPool.hasDuplicateTask(task)) {
                            return false;
                        }

                        if (taskPool.hasCollideTask(task)) {
                            SyncTask merged = taskPool.merge(task);
                            queue.addTask(merged);
                            return false;
                        }

                        return false;
                    }

                };

                source.start();
                dataSources.add(source);


            } catch (Exception e) {
                System.out.println("Error starting DBConsumer " + i);
                e.printStackTrace();
                break;
            }


        }


    }
}
