package com.cozystay;

import com.cozystay.dts.AbstractDataSourceImpl;
import com.cozystay.dts.DataSource;
import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;
import com.cozystay.model.SyncTaskBuilder;
import com.cozystay.structure.ProcessedTaskPool;
import com.cozystay.structure.SimpleProcessedTaskPool;
import com.cozystay.structure.SimpleWorkerQueueImpl;
import com.cozystay.structure.WorkerQueue;

import java.text.ParseException;
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
                if(!pool.hasTask(newRecord)){
                    pool.add(newRecord);
                    return;
                }
                SyncTask currentTask = pool.get(newRecord.getId());
                SyncTask mergedTask = currentTask .merge(newRecord);
                if(mergedTask.allOperationsCompleted()){
                    pool.remove(currentTask);
                }else{
                    pool.add(mergedTask);
                }

            }

            @Override
            public void workOn() {
                SyncTask toProcess = pool.poll();
                if(toProcess==null){
                    return;
                }

                if(toProcess.allOperationsCompleted()) {
                    pool.remove(toProcess);
                }

                for (DataSource source :
                        dataSources) {
                    for(SyncOperation operation: toProcess.getOperations()){
                        if(operation.shouldSendToSource(source.getName())){
                            source.writeDB(operation);
                            operation.setSourceSend(source.getName());
                        }
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

                };

                source.start();
                dataSources.add(source);
                SyncTaskBuilder.addSource(source.getName());


            } catch (ParseException e) {
                System.out.println("Could not find DBConsumer" + i);
                System.out.println("Running with " + (i-1)+" consumers");

                break;
            }catch (Exception e){
                e.printStackTrace();
            }


        }


    }
}
