package com.cozystay;

import com.cozystay.datasource.BinLogDataSourceImpl;
import com.cozystay.datasource.DataSource;
import com.cozystay.model.SyncTask;
import com.cozystay.notify.HttpSyncNotifierImpl;
import com.cozystay.notify.SyncNotifier;
import com.cozystay.structure.*;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;

public class NotifyDaemon implements Daemon {

    private static Logger logger = LoggerFactory.getLogger(NotifyDaemon.class);

    private static TaskRunner primaryRunner;
    private static DataSource source;
    private static SyncNotifier notifier;


    private static void onInitNotify(DaemonContext daemonContext) throws IOException, ParseException {
        logger.info("DB Sync primaryRunner launched");
        Properties prop = new Properties();
        prop.load(NotifyDaemon.class.getResourceAsStream("/notify-config.properties"));
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

        notifier = new HttpSyncNotifierImpl();
        notifier.loadRules(prop);

        final TaskPool primaryPool = new RedisTaskPoolImpl(redisHost,
                redisPort,
                redisPassword,
                QueueConstants.DATA_NOTIFY_HASH_KEY,
                QueueConstants.DATA_NOTIFY_SET_KEY);

        primaryRunner = new SimpleTaskRunnerImpl(1, threadNumber) {

            @Override
            public void workOn() {
                SyncTask toProcess;
                synchronized (primaryPool) {
                    toProcess = primaryPool.poll();
                    if (toProcess == null) {
                        return;
                    }

                    if (notifier.matchTask(toProcess)) {
                        notifier.notify(toProcess);
                    }

                }
            }
        };

        try {
            source = new BinLogDataSourceImpl(prop, "db") {

                @Override
                public void consumeData(SyncTask task) {
                    synchronized (primaryPool) {
                        if (!primaryPool.hasTask(task)) {
                            logger.info("add new task to pool: {}" + task.toString());
                            primaryPool.add(task);
                            return;
                        }
                        SyncTask existingTask = primaryPool.get(task.getId());
                        SyncTask mergedTask = existingTask.deepMerge(task);
                        primaryPool.remove(existingTask);
                        primaryPool.add(mergedTask);
                        logger.info("add merged task to pool: {}" + mergedTask.toString());
                    }


                }
            };
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    private static void onStartNotify() {
        primaryRunner.start();
        source.start();
    }

    private static void onStopNotify() {
        primaryRunner.stop();
        source.stop();

    }

    public static void main(String[] args) throws Exception {

        onInitNotify(null);
        onStartNotify();


        // Signal handler method for CTRL-C and simple kill command.
        Signal.handle(new Signal("TERM"), signal -> onStopNotify());
        // Signal handler method for kill -INT command
        Signal.handle(new Signal("INT"), signal -> onStopNotify());

        // Signal handler method for kill -HUP command
        Signal.handle(new Signal("HUP"), signal -> onStopNotify());
    }


    @Override
    public void init(DaemonContext daemonContext) throws Exception {
        onInitNotify(daemonContext);
    }

    @Override
    public void start() throws Exception {
        onStartNotify();

    }

    @Override
    public void stop() throws Exception {
        onStopNotify();
    }

    @Override
    public void destroy() {


    }
}
