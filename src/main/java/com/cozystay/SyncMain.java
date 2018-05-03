package com.cozystay;

import com.cozystay.datasource.BinLogDataSourceImpl;
import com.cozystay.datasource.DataSource;
import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;
import com.cozystay.model.SyncTaskBuilder;
import com.cozystay.structure.ProcessedTaskPool;
import com.cozystay.structure.RedisProcessedTaskPool;
import com.cozystay.structure.SimpleTaskRunnerImpl;
import com.cozystay.structure.TaskRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SyncMain {
    @SuppressWarnings("FieldCanBeLocal")
    private static Logger logger = LoggerFactory.getLogger(SyncMain.class);
    private static int MAX_DATABASE_SIZE = 10;
    final static List<DataSource> dataSources = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        System.out.print("DB Sync runner launched");
        final Properties prop = new Properties();
        prop.load(SyncMain.class.getResourceAsStream("/db-config.properties"));
        Integer threadNumber = Integer.valueOf(prop.getProperty("threadNumber", "5"));
        System.out.printf("Running with %d threads%n", threadNumber);



        String redisHost;
        if ((redisHost = prop.getProperty("redis.host")) == null) {
            throw new ParseException("redis.host", 6);
        }

        Integer redisPort;
        if ((redisPort = Integer.valueOf(prop.getProperty("redis.port"))) <= 0) {
            throw new ParseException("redis.port", 7);
        }
        String redisPassword;
        if ((redisPassword = prop.getProperty("redis.password")) == null) {
            throw new ParseException("redis.password", 8);
        }
        final ProcessedTaskPool pool = new RedisProcessedTaskPool(redisHost, redisPort, redisPassword);


        final TaskRunner runner = new SimpleTaskRunnerImpl(1, threadNumber) {

            @Override
            public void addTask(SyncTask newRecord) {
                synchronized (pool) {
                    if (!pool.hasTask(newRecord)) {
                        pool.add(newRecord);
                        return;
                    }
                    SyncTask currentTask = pool.get(newRecord.getId());
                    SyncTask mergedTask = currentTask.merge(newRecord);
                    if (mergedTask.allOperationsCompleted()) {
                        pool.remove(currentTask);
                    } else {
                        pool.add(mergedTask);
                    }
                }

            }

            @Override
            public void workOn() {
                SyncTask toProcess;
                synchronized (pool) {
                    toProcess = pool.poll();
                    if (toProcess == null) {
//                        System.out.println("No SyncTask to process");
                        return;
                    }


                    for (DataSource source :
                            dataSources) {
                        if (!source.isRunning()) {
                            continue;
                        }
                        for (SyncOperation operation : toProcess.getOperations()) {
                            if (operation.shouldSendToSource(source.getName())) {
                                if (source.writeDB(operation)) {
                                    operation.updateStatus(source.getName(), SyncOperation.SyncStatus.SEND);
                                    logger.info("write operation {} to source {} succeed.",
                                            operation.toString(),
                                            source.getName());
                                } else {
                                    operation.updateStatus(source.getName(), SyncOperation.SyncStatus.COMPLETED);
                                    logger.error("write operation {} to source {} failed and skipped. ",
                                            operation.toString(),
                                            source.getName());


                                }
                            }
                        }
                    }
                    if (!toProcess.allOperationsCompleted()) {
                        pool.add(toProcess);
                    }
                }
            }
        };
        runner.start();


        for (int i = 1; i <= MAX_DATABASE_SIZE; i++) {
            final int currentIndex = i;
            try {
                final DataSource source = new BinLogDataSourceImpl(prop, "db" + currentIndex) {
                    @Override
                    public void consumeData(SyncTask task) {
                        runner.addTask(task);

                    }

                };
                dataSources.add(source);
                SyncTaskBuilder.addSource(source.getName());



            } catch (ParseException e) {
                System.out.printf("Could not find DBConsumer%d, Running with %d consumers%n", currentIndex, currentIndex - 1);

                break;
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        for (DataSource source : dataSources) {
            source.init();
        }
        for (DataSource source : dataSources) {
            source.start();
        }

        Signal.handle(new Signal("TERM"), new SignalHandler() {
            // Signal handler method for CTRL-C and simple kill command.
            public void handle(Signal signal) {
                SyncMain.onStop();
            }
        });
        Signal.handle(new Signal("INT"), new SignalHandler() {
            // Signal handler method for kill -INT command

            public void handle(Signal signal) {
                SyncMain.onStop();
            }
        });

        Signal.handle(new Signal("HUP"), new SignalHandler() {
            // Signal handler method for kill -HUP command
            public void handle(Signal signal) {
                SyncMain.onStop();
            }
        });


//        Thread.sleep(1000 * 60 * 24);

    }

    private static void onStop(){
        for (DataSource source : dataSources) {
            System.out.println("stop source " + source.getName());
            source.stop();
        }
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            System.exit(130);

        }
    }
}
