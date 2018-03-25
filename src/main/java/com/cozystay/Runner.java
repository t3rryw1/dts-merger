package com.cozystay;

import com.aliyun.drc.clusterclient.message.ClusterMessage;
import com.cozystay.dts.AbstractDataSourceImpl;
import com.cozystay.dts.DataSource;
import com.cozystay.model.SyncTask;
import com.cozystay.model.SyncTaskBuilder;
import com.cozystay.structure.ProcessedTaskPool;
import com.cozystay.structure.SimpleProcessedTaskPool;
import com.cozystay.structure.SimpleWorkerQueueImpl;
import com.cozystay.structure.WorkerQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Runner {
    @SuppressWarnings("FieldCanBeLocal")
    private static int MAX_DATABASE_SIZE = 10;

    public static void main(String[] args) throws Exception {
        System.out.print("DB Sync runner launched");
        final List<DataSource> dataSources = new ArrayList<>();
        final ProcessedTaskPool pool = new SimpleProcessedTaskPool();

        final WorkerQueue queue = new SimpleWorkerQueueImpl(1, 20) {

            @Override
            public void addTask(SyncTask newRecord) {
                if(!pool.hasCollide(newRecord)){
                    pool.add(newRecord);
                    return;
                }
                SyncTask currentTask = pool.get(newRecord.getId());
                if (currentTask.hasDone(newRecord)) {
                    currentTask.setSourceFinished(newRecord.getSource());
                    if (currentTask.allSourcesFinished()) {
                        pool.remove(currentTask);
                    } else {
                        pool.add(currentTask);
                    }
                } else {
                    SyncTask mergedTask = currentTask.merge(newRecord);
                    pool.add(mergedTask);
                }
            }

            @Override
            public void workOn() {
                SyncTask toProcess = pool.poll();

                if (toProcess.allSourcesFinished()) {
                    pool.remove(toProcess);
                    return;
                }
                for (DataSource source :
                        dataSources) {
                    if (toProcess.shouldWriteSource(source.getName())) {
                        source.writeDB(toProcess);
                        toProcess.setSourceWritten(source.getName());
                    }
                }
                pool.add(toProcess);
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


                    public boolean shouldFilterMessage(ClusterMessage message) {
                        //TODO: filter out useless messages


                        if (message.getRecord().getTablename() == null) {
                            return true;
                        }
                        if (message.getRecord().getDbname() == null) {
                            return true;
                        }
//
//                    if (message.getRecord().getTablename().equals("calendar")) {
//                        message.ackAsConsumed();
//                        continue;
//                    }
//
//                    /* 可打印数据 */
//                    logger.error(message.getRecord().getDbname() + ":"
//                            + message.getRecord().getTablename() + ":"
//                            + message.getRecord().getOpt() + ":"
//                            + message.getRecord().getTimestamp() + ":"
//                            + message.getRecord());
//
//
                        return false;
                    }

                };

                source.start();
                dataSources.add(source);
                SyncTaskBuilder.addSource(source.getName());


            } catch (Exception e) {
                System.out.println("Error starting DBConsumer " + i);
                e.printStackTrace();
                break;
            }


        }


    }
}
