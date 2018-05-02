package com.cozystay;

import com.cozystay.datasource.BinLogDataSourceImpl;
import com.cozystay.datasource.DataSource;
import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;
import com.cozystay.structure.ProcessedTaskPool;
import com.cozystay.structure.RedisProcessedTaskPool;
import com.cozystay.structure.SimpleTaskRunnerImpl;
import com.cozystay.structure.TaskRunner;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SyncDaemon implements Daemon {


    TaskRunner runner;
    Properties prop;
    @SuppressWarnings("FieldCanBeLocal")
    private static int MAX_DATABASE_SIZE = 10;
    final static List<DataSource> dataSources = new ArrayList<>();

    @Override
    public void init(DaemonContext daemonContext) throws DaemonInitException, Exception {
        System.out.print("DB Sync runner launched");
        prop = new Properties();
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

        runner = new SimpleTaskRunnerImpl(1, threadNumber) {

            @Override
            public void addTask(SyncTask newRecord) {
                synchronized (pool) {
                    System.out.println("add new task: " + newRecord.toString());
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
                        return;
                    }
                    System.out.println("work on task: " + toProcess.toString());

                    for (DataSource source :
                            dataSources) {
                        for (SyncOperation operation : toProcess.getOperations()) {
                            if (operation.shouldSendToSource(source.getName())) {
                                System.out.printf("write operation %s to source %s %n: ",
                                        operation.toString(),
                                        source.getName());
                                source.writeDB(operation);
                                operation.setSourceSend(source.getName());
                            }
                        }
                    }
                    if (!toProcess.allOperationsCompleted()) {
                        pool.add(toProcess);
                    }
                }
            }
        };

        for (int i = 1; i <= MAX_DATABASE_SIZE; i++) {
            final int currentIndex = i;
            try {
                DataSource source = new BinLogDataSourceImpl(prop, "db" + currentIndex) {
                    @Override
                    public void consumeData(SyncTask task) {
                        runner.addTask(task);
                    }

                };
                dataSources.add(source);


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

    }

    @Override
    public void start() {
        System.out.println("start");
        runner.start();
        for (DataSource source : dataSources) {
            source.start();
        }

    }

    @Override
    public void stop() {
        System.out.println("stop");
        for (DataSource source : dataSources) {
            System.out.println("stop source " + source.getName());
            source.stop();
        }
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            System.out.println("stopped sources ");

        }
        runner.stop();

    }

    @Override
    public void destroy() {

        System.out.println("destroy");

    }


}
