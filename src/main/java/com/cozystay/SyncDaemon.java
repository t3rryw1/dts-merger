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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SyncDaemon implements Daemon {

    private static Logger logger = LoggerFactory.getLogger(SyncDaemon.class);

    private TaskRunner runner;
    private Properties prop;
    @SuppressWarnings("FieldCanBeLocal")
    private static int MAX_DATABASE_SIZE = 10;
    private final static List<DataSource> dataSources = new ArrayList<>();

    @Override
    public void init(DaemonContext daemonContext) throws DaemonInitException, Exception {
        logger.info("DB Sync runner launched");
        prop = new Properties();
        prop.load(SyncMain.class.getResourceAsStream("/db-config.properties"));
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

        final ProcessedTaskPool pool = new RedisProcessedTaskPool(redisHost, redisPort, redisPassword);

        runner = new SimpleTaskRunnerImpl(1, threadNumber) {

            @Override
            public void addTask(SyncTask newRecord) {
                synchronized (pool) {
                    if (!pool.hasTask(newRecord)) {
                        pool.add(newRecord);
                        logger.info("add new task: {}" + newRecord.toString());
                        return;
                    }

                    SyncTask currentTask = pool.get(newRecord.getId());
                    SyncTask mergedTask = currentTask.merge(newRecord);
                    pool.remove(currentTask);
                    if (!mergedTask.allOperationsCompleted()) {
                        pool.add(mergedTask);
                        logger.info("add merged task: {}" + mergedTask.toString());
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
//                    logger.info("work on task: {}" + toProcess.toString());

                    for (DataSource source :
                            dataSources) {
                        if (!source.isRunning()) {
                            continue;
                        }
                        for (SyncOperation operation : toProcess.getOperations()) {
                            if (operation.shouldSendToSource(source.getName())) {
                                try{
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
                                }catch (SQLException e){
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
        } finally {
            System.out.println("stopped sources ");

        }
        runner.stop();

    }

    @Override
    public void destroy() {

        System.out.println("destroy");

    }


}
