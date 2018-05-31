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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SyncDaemon implements Daemon {

    private static Logger logger = LoggerFactory.getLogger(SyncDaemon.class);

    private static TaskRunner primaryRunner;
    private static TaskRunner secondaryRunner;
    @SuppressWarnings("FieldCanBeLocal")
    private static int MAX_DATABASE_SIZE = 10;
    private final static List<DataSource> dataSources = new ArrayList<>();

    private static void onInitSync(DaemonContext daemonContext) throws Exception {
        logger.info("DB Sync primaryRunner launched");
        Properties prop = new Properties();
        prop.load(SyncDaemon.class.getResourceAsStream("/db-config.properties"));
        Integer threadNumber = Integer.valueOf(prop.getProperty("threadNumber", "5"));
        logger.info("Running with {} threads", threadNumber);


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

        final ProcessedTaskPool primaryPool = new RedisProcessedTaskPool(redisHost,
                redisPort,
                redisPassword,
                RedisProcessedTaskPool.DATA_PRIMARY_HASH_KEY,
                RedisProcessedTaskPool.DATA_PRIMARY_SET_KEY);


        final ProcessedTaskPool secondaryPool = new RedisProcessedTaskPool(redisHost,
                redisPort,
                redisPassword,
                RedisProcessedTaskPool.DATA_SECONDARY_HASH_KEY,
                RedisProcessedTaskPool.DATA_SECONDARY_SET_KEY);

        primaryRunner = new SimpleTaskRunnerImpl(1, threadNumber) {

            @Override
            public void workOn() {
                SyncTask toProcess;
                synchronized (primaryPool) {
                    toProcess = primaryPool.poll();
                    if (toProcess == null) {
                        return;
                    }
//                    logger.info("work on task: {}" + toProcess.toString());

                    for (DataSource source :
                            dataSources) {
                        if (!source.isRunning()) {
                            continue;
                        }
                        for (SyncOperation operation : toProcess.getOperations()) {
                            if (operation.shouldSendToSource(source.getName())) {
                                try {
                                    if (source.writeDB(operation)) {
                                        operation.updateStatus(source.getName(), SyncOperation.SyncStatus.SEND);
                                        logger.info("write operation {} to source {} succeed.",
                                                operation.toString(),
                                                source.getName());
                                    } else {
                                        operation.updateStatus(source.getName(), SyncOperation.SyncStatus.COMPLETED);
                                        logger.error("wrote operation {} to source {} but return no result. ",
                                                operation.toString(),
                                                source.getName());
                                    }
                                } catch (SQLException e) {
                                    operation.updateStatus(source.getName(), SyncOperation.SyncStatus.COMPLETED);
                                    logger.error("write operation {} to source {} failed and skipped. ",
                                            operation.toString(),
                                            source.getName());

                                }

                            }
                        }
                    }
                    if (!toProcess.allOperationsCompleted()) {
                        primaryPool.add(toProcess);
                    }
                }
            }
        };

        secondaryRunner = new SimpleTaskRunnerImpl(1, 2) {

            @Override
            public void workOn() {
                SyncTask task;
                synchronized (secondaryPool) {
                    task = secondaryPool.poll();
                    if (task == null) {
                        return;
                    }
                }
                synchronized (primaryPool) {
                    if (!primaryPool.hasTask(task)) {
                        logger.info("add task  to primary pool: {}" + task.toString());
                        primaryPool.add(task);
                        return;
                    }
                }
                synchronized (secondaryPool){
                    logger.info("add task back to second pool: {}" + task.toString());
                    addTaskToSecondaryQueue(secondaryPool,task);
                }
            }
        };

        for (int i = 1; i <= MAX_DATABASE_SIZE; i++) {
            final int currentIndex = i;
            try {
                DataSource source = new BinLogDataSourceImpl(prop, "db" + currentIndex) {
                    @Override
                    public void consumeData(SyncTask newTask) {
                        if (newTask.getOperations().size() > 1) {
                            logger.error("new task should not have multiple operations: {}" + newTask.toString());
                            return;
                        }
                        synchronized (primaryPool) {

                            if (!primaryPool.hasTask(newTask)) {
                                primaryPool.add(newTask);
                                logger.info("add new task: {}" + newTask.toString());
                                return;
                            }

                            SyncTask currentTask = primaryPool.get(newTask.getId());
                            if (currentTask.canMergeStatus(newTask.firstOperation())) {
                                SyncTask mergedTask = currentTask.mergeStatus(newTask);
                                primaryPool.remove(currentTask);
                                if (!mergedTask.allOperationsCompleted()) {
                                    primaryPool.add(mergedTask);
                                    logger.info("add merged task: {}" + mergedTask.toString());
                                } else {
                                    logger.info("removed task: {}" + mergedTask.toString());

                                }
                                return;
                            }
                        }

                        synchronized (secondaryPool) {
                            logger.info("add new task to second pool: {}" + newTask.toString());
                            addTaskToSecondaryQueue(secondaryPool, newTask);
                        }
                    }

                };
                dataSources.add(source);


            } catch (ParseException e) {
                logger.info("Could not find DBConsumer {}, Running with {} consumers%n", currentIndex, currentIndex - 1);

                break;
            } catch (Exception e) {
                e.printStackTrace();
            }


        }

        for (DataSource source : dataSources) {
            source.init();
        }

    }

    private static void onStartSync() {
        System.out.println("start");
        primaryRunner.start();
        secondaryRunner.start();
        for (DataSource source : dataSources) {
            source.start();
        }

    }



    private static void onStopSync() {
        System.out.println("stop");
        for (DataSource source : dataSources) {
            System.out.println("stop source " + source.getName());
            source.stop();
        }
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            System.out.println("stopped sources ");

        }
        primaryRunner.stop();
        secondaryRunner.stop();


    }

    private static void addTaskToSecondaryQueue(ProcessedTaskPool taskPool, SyncTask task){
        if (!taskPool.hasTask(task)) {
            taskPool.add(task);

            return;
        }
        SyncTask currentTask = taskPool.get(task.getId());
        currentTask = currentTask.deepMerge(task);
        logger.info("add merged task to second pool: {}" + currentTask.toString());
        taskPool.remove(currentTask);
        taskPool.add(currentTask);

    }

    @Override
    public void init(DaemonContext daemonContext) throws Exception {
        onInitSync(daemonContext);
    }

    @Override
    public void start() {
        onStartSync();
    }


    @Override
    public void stop() {
        onStopSync();
    }

    @Override
    public void destroy() {


    }

    public static void main(String[] args) throws Exception {

        onInitSync(null);
        onStartSync();


        Signal.handle(new Signal("TERM"), new SignalHandler() {
            // Signal handler method for CTRL-C and simple kill command.
            public void handle(Signal signal) {
                onStopSync();
            }
        });
        Signal.handle(new Signal("INT"), new SignalHandler() {
            // Signal handler method for kill -INT command

            public void handle(Signal signal) {
                onStopSync();
            }
        });

        Signal.handle(new Signal("HUP"), new SignalHandler() {
            // Signal handler method for kill -HUP command
            public void handle(Signal signal) {
                onStopSync();
            }
        });
    }


}
