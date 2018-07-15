package com.cozystay;

import com.cozystay.datasource.BinLogDataSourceImpl;
import com.cozystay.datasource.DataSource;
import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;
import com.cozystay.structure.*;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class SyncDaemon implements Daemon {

    private static Logger logger = LoggerFactory.getLogger(SyncDaemon.class);

    private static TaskRunner queueRunner;
    private static TaskRunner primaryRunner;
    private static TaskRunner secondaryRunner;
    private static TaskRunner doneRunner;
    @SuppressWarnings("FieldCanBeLocal")
    private static int MAX_DATABASE_SIZE = 10;
    private final static List<DataSource> dataSources = new ArrayList<>();

    private static void onInitSync(DaemonContext daemonContext) throws Exception {
        logger.info("DB Sync primaryRunner launched");
        Properties prop = new Properties();
        prop.load(SyncDaemon.class.getResourceAsStream("/db-config.properties"));
        Integer threadNumber = Integer.valueOf(prop.getProperty("threadNumber", "5"));
        logger.info("Running with {} threads", threadNumber);

        Integer expiredTime = Integer.valueOf(prop.getProperty("expiredTime", "36000"));
        logger.info("Current expiring time {} ", expiredTime);

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

        final ProcessedTaskPool donePool = new RedisProcessedTaskPool(redisHost,
                redisPort,
                redisPassword,
                RedisProcessedTaskPool.DATA_SEND_HASH_KEY,
                RedisProcessedTaskPool.DATA_SEND_SET_KEY);


        final TaskQueue todoQueue = new RedisTaskQueue(redisHost,
                redisPort,
                redisPassword,
                RedisProcessedTaskPool.DATA_QUEUE_KEY);

        primaryRunner = new SimpleTaskRunnerImpl(1, threadNumber) {

            @Override
            public void workOn() {
                SyncTask toProcess;
                synchronized (primaryPool) {
                    if ((toProcess = primaryPool.poll()) == null) {
                        return;
                    }
                }

                for (DataSource source :
                        dataSources) {
                    if (!source.isRunning()) {
                        continue;
                    }
                    for (SyncOperation operation : toProcess.getOperations()) {
                        if (operation.shouldSendToSource(source.getName())) {
                            try {
                                logger.info("proceed to executing sql ");

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

                if (toProcess.allOperationsCompleted()) {
                    logger.info("removed completed task: {}", toProcess.toString());
                    return;
                }

                synchronized (donePool) {


                    if (!donePool.hasTask(toProcess)) {
                        donePool.add(toProcess);
                        logger.info("add task to done pool: {}", toProcess.toString());
                        return;

                    }
                    SyncTask currentTask = donePool.get(toProcess.getId());
                    if (!currentTask.canMergeStatus(toProcess.firstOperation())) {
                        logger.error("removed faulty task: {}", toProcess.toString());
                        return;
                    }
                    SyncTask mergedTask = currentTask.mergeStatus(toProcess);
                    donePool.remove(currentTask);
                    if (mergedTask.allOperationsCompleted()) {
                        logger.info("removed task: {}", mergedTask.toString());
                        return;
                    }
                    donePool.add(mergedTask);
                    logger.info("add merged task : {}", mergedTask.toString());


                }

            }
        };

        doneRunner = new SimpleTaskRunnerImpl(1, 5) {

            @Override
            public void workOn() {
                SyncTask toProcess;

                synchronized (donePool) {
                    if ((toProcess = donePool.poll()) == null) {
                        return;
                    }

                    SyncOperation lastOperation = toProcess.firstOperation();

                    if (lastOperation != null
                            &&
                            !lastOperation.getSyncStatus().values().contains(SyncOperation.SyncStatus.INIT)
                            &&
                            lastOperation.getTime() + 1000 * expiredTime < new Date().getTime()) {
                        logger.error("removed expired task: {}, date {}", toProcess.toString(),
                                new SimpleDateFormat().format(new Date(lastOperation.getTime())));
                        return;
                    }
                    donePool.add(toProcess);

                }

            }
        };


        secondaryRunner = new SimpleTaskRunnerImpl(1, 5) {

            @Override
            public void workOn() {
                SyncTask task;
                synchronized (secondaryPool) {
                    synchronized (primaryPool) {
                        if ((task = secondaryPool.poll()) == null) {
                            return;
                        }

                        if (primaryPool.hasTask(task) || donePool.hasTask(task)) {
                            secondaryPool.add(task);
                        } else {
                            primaryPool.add(task);
                            logger.info("add task to primary pool and remove from secondary pool: {}",
                                    task.toString());

                        }
                    }
                }
            }
        };

        queueRunner = new SimpleTaskRunnerImpl(1, 5) {

            @Override
            public void workOn() {

                SyncTask newTask;
                synchronized (todoQueue) {
                    if ((newTask = todoQueue.pop()) == null) {
                        return;
                    }
                }
                logger.info("begin to work on new task: {}", newTask.toString());

                synchronized (secondaryPool) {
                    synchronized (primaryPool) {

                        synchronized (donePool) {
                            if (donePool.hasTask(newTask)) {
                                SyncTask currentTask = donePool.get(newTask.getId());
                                if (!currentTask.canMergeStatus(newTask.firstOperation())) {
                                    logger.info("add new task to second pool: {}" + newTask.toString());
                                    addTaskToSecondaryQueue(secondaryPool, newTask);
                                    return;

                                }
                                SyncTask mergedTask = currentTask.mergeStatus(newTask);
                                donePool.remove(currentTask);
                                if (mergedTask.allOperationsCompleted()) {
                                    logger.info("removed task: {}", mergedTask.toString());
                                    return;
                                }
                                donePool.add(mergedTask);
                                logger.info("add merged task : {}", mergedTask.toString());
                                return;

                            }
                        }
                        if (!primaryPool.hasTask(newTask)) {
                            primaryPool.add(newTask);
                            logger.info("add new task to primary queue: {}", newTask.toString());
                            return;
                        }
                    }

                    logger.info("add new task to second pool: {}" + newTask.toString());
                    addTaskToSecondaryQueue(secondaryPool, newTask);

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
                            logger.error("new task should not have multiple operations: {}", newTask.toString());
                            return;
                        }
                        synchronized (todoQueue) {
                            todoQueue.push(newTask);
                            logger.info("add new task to queue: {}", newTask.toString());
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

        for (
                DataSource source : dataSources)

        {
            source.init();
        }

    }

    private static void onStartSync() {
        System.out.println("start");
        queueRunner.start();
        primaryRunner.start();
        secondaryRunner.start();
        doneRunner.start();
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
        queueRunner.stop();
        primaryRunner.stop();
        secondaryRunner.stop();
        doneRunner.stop();


    }

    private static void addTaskToSecondaryQueue(ProcessedTaskPool taskPool, SyncTask task) {
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


        // Signal handler method for CTRL-C and simple kill command.
        Signal.handle(new Signal("TERM"), signal -> onStopSync());
        // Signal handler method for kill -INT command
        Signal.handle(new Signal("INT"), signal -> onStopSync());

        // Signal handler method for kill -HUP command
        Signal.handle(new Signal("HUP"), signal -> onStopSync());
    }


}
