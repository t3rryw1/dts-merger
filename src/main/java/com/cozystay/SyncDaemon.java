package com.cozystay;

import com.cozystay.datasource.BinLogDataSourceImpl;
import com.cozystay.datasource.DataSource;
import com.cozystay.db.DBRunner;
import com.cozystay.db.SimpleDBRunnerImpl;
import com.cozystay.model.*;
import com.cozystay.structure.TaskQueue;
import com.cozystay.structure.*;
import javafx.util.Pair;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import sun.misc.Signal;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;

@CommandLine.Command(description = "Diffs the database by a starting time, and eliminate the difference",
        name = "db-merger-cli", mixinStandardHelpOptions = true, version = "checksum 3.0")
public class SyncDaemon implements Daemon, Callable<Void> {


    private static Logger logger = LoggerFactory.getLogger(SyncDaemon.class);

    private static TaskRunner queueRunner;
    private static TaskRunner primaryRunner;
    private static TaskRunner secondaryRunner;
    private static TaskRunner doneRunner;
    @SuppressWarnings("FieldCanBeLocal")
    private static int MAX_DATABASE_SIZE = 10;
    private final static List<DataSource> dataSources = new ArrayList<>();

    private static String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

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

        final TaskPool primaryPool = new RedisTaskPoolImpl(redisHost,
                redisPort,
                redisPassword,
                QueueConstants.DATA_PRIMARY_HASH_KEY,
                QueueConstants.DATA_PRIMARY_SET_KEY);


        final TaskPool secondaryPool = new RedisTaskPoolImpl(redisHost,
                redisPort,
                redisPassword,
                QueueConstants.DATA_SECONDARY_HASH_KEY,
                QueueConstants.DATA_SECONDARY_SET_KEY);

        final TaskPool donePool = new RedisTaskPoolImpl(redisHost,
                redisPort,
                redisPassword,
                QueueConstants.DATA_SEND_HASH_KEY,
                QueueConstants.DATA_SEND_SET_KEY);


        final TaskQueue todoQueue = new RedisTaskQueueImpl(redisHost,
                redisPort,
                redisPassword,
                QueueConstants.DATA_QUEUE_KEY);

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
                    toProcess.getOperations().forEach(operation -> {
                        if (operation.shouldSendToSource(source.getName())) {
                            try {
                                logger.info("proceed to executing sql ");
                                int result = source.writeDB(operation);
                                if (result > 0) {
                                    operation.updateStatus(source.getName(), SyncOperation.SyncStatus.SEND);
                                    logger.info("write operation {} to source {} succeed.",
                                            operation.toString(),
                                            source.getName());
                                } else if (result == 0) {
                                    operation.updateStatus(source.getName(), SyncOperation.SyncStatus.COMPLETED);
                                    logger.error("wrote operation {} to source {} but return no result. ",
                                            operation.toString(),
                                            source.getName());
                                } else {
                                    //if connection error, retry this query.
                                    synchronized (primaryPool) {
                                        logger.error(" add operation {} back and retry later.",
                                                operation.toString());
                                        primaryPool.add(toProcess);
                                        return;
                                    }
                                }
                            } catch (SQLException e) {
                                operation.updateStatus(source.getName(), SyncOperation.SyncStatus.COMPLETED);
                                logger.error("write operation {} to source {} failed and skipped. ",
                                        operation.toString(),
                                        source.getName());

                            }

                        }
                    });

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

        dataSources.forEach(DataSource::init);

    }

    private static void onStartSync() {
        System.out.println("start");
        queueRunner.start();
        primaryRunner.start();
        secondaryRunner.start();
        doneRunner.start();
        dataSources.forEach(DataSource::start);
    }


    private static void onStopSync() {
        System.out.println("stop");
        dataSources.forEach(source -> {
            System.out.println("stop source " + source.getName());
            source.stop();
        });
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

    private static void addTaskToSecondaryQueue(TaskPool taskPool, SyncTask task) {
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


    @Option(names = {"-t", "--table"}, required = true, description = "table name to sync for")
    private String tableName;

    @Option(names = {"-d", "--database"}, required = true, description = "database name to sync for")
    private String dataBase;

    @Option(names = {"-o", "--order"}, description = "specify the order key")
    private String orderBy;

    @Option(names = {"-c", "--condition"}, arity = "0..*", description = "specify filter conditions, use format as 'updated_at|>=|2016-01-01 12:00:00'")
    private String[] conditions = {};

    @Option(names = {"-y"}, description = "proceed without prompt")
    private boolean noPrompt;

    @Option(names = {"-s", "--silent"}, description = "do not print SQL detail")
    private boolean silent;

    @Option(names = {"-e", "--execute"}, description = "Actually modify data, if this flag is turned off then only print sql without executing")
    private boolean execute;

    @Option(names = {"-k", "--keys"}, arity = "0..*", description = "table's index keys")
    private String[] keys = {};


    public static void main(String[] args) throws Exception {

        if (args.length == 1 && args[0].equals("--daemon")) {
            onInitSync(null);
            onStartSync();

            // Signal handler method for CTRL-C and simple kill command.
            Signal.handle(new Signal("TERM"), signal -> onStopSync());
            // Signal handler method for kill -INT command
            Signal.handle(new Signal("INT"), signal -> onStopSync());

            // Signal handler method for kill -HUP command
            Signal.handle(new Signal("HUP"), signal -> onStopSync());
            return;
        }

        CommandLine.call(new SyncDaemon(), args);
    }

    private void enterToContinue() {
        Scanner s = new Scanner(System.in);

        System.out.println("Press Enter to continue...");

        s.nextLine();
    }

    @Override
    public Void call() throws Exception {


        Properties prop = new Properties();
        prop.load(SyncDaemon.class.getResourceAsStream("/db-config.properties"));

        List<Pair<DBRunner, DataItemList>> diffList = new ArrayList<>();
        String orderByKey = orderBy != null ? orderBy : "updated_at";
//        String orderByKeyType = prop.getProperty("schema.order_by_key_type", "timestamp");
        List<String> conditionList = Arrays.asList(conditions);
        List<String> keyList = new ArrayList<>(Arrays.asList(keys));
        if (keyList.size() == 0) {
            keyList.add("id");
        }
        if (conditionList.size() > 0) {
            System.out.format("[Info] Will Perform by-line comparison in table %s.%s, order by %s field, indexed by %s, filters:",
                    this.dataBase,
                    this.tableName,
                    orderByKey,
                    keyList);
            System.out.println(conditionList.toString());

        } else {
            System.out.format("[Info] Will Perform by-line comparison in table %s.%s, order by %s field, indexed by %s, no filter\n",
                    this.dataBase,
                    this.tableName,
                    orderByKey,
                    keyList);
        }
        if (!noPrompt) {
            enterToContinue();
        }

        DataItemList mergedList = null;
        for (int i = 1; i <= MAX_DATABASE_SIZE; i++) {
            DBRunner source = null;
            try {
                source = new SimpleDBRunnerImpl(prop, "db" + i, silent);

                FetchOperation fetchOperation = new FetchOperationImpl(source, orderByKey, silent, keyList);
                fetchOperation
                        .setDB(dataBase)
                        .setTable(tableName)
                        .setConditions(conditionList);
                DataItemList list = fetchOperation.fetchList();
                if (mergedList == null) {
                    mergedList = list;
                } else {
                    mergedList = mergedList.merge(list);
                }

                diffList.add(new Pair<>(source, list));
            } catch (ParseException e) {
                logger.info("Could not find DBRunner {}, Running with {} runners%n", i, i - 1);
                break;
            } catch (IllegalArgumentException e) {
                System.err.format("[Error] Error parsing conditions: %s\n",
                        e.getMessage());
                return null;
            } catch (Exception e) {
                System.err.format("[Error] Error while performing query in datasource %s\n",
                        Objects.requireNonNull(source).getDBInfo());
                e.printStackTrace();
                return null;
            }

        }
        if (!noPrompt) {
            enterToContinue();
        }
        System.out.format("[Info] Diff merged data with data in each data source\n");

        for (Pair<DBRunner, DataItemList> list : diffList) {
            List<String> updatedList = new LinkedList<>();
            DataItemList toChangeList = mergedList.diff(list.getValue());
            DBRunner runner = list.getKey();
            if (toChangeList.size() == 0) {
                System.out.format("[Info] No diff found in data source %s\n", runner.getDBInfo());
                continue;
            }
            System.out.format("[Info] Found %d entries to update/insert for data source %s\n",
                    toChangeList.size(),
                    runner.getDBInfo());
            toChangeList.forEach(item -> {
                String updateSql = item.getUpdateSql(tableName);
                if (!silent) {
                    System.out.format("[Info] will Execute SQL >>>\n %s  \n", updateSql);
                }
                if (!noPrompt) {
                    enterToContinue();
                }
                if (execute) {

                    if (!runner.update(dataBase, updateSql)) {
                        if(!runner.update(dataBase, updateSql.replace("INSERT", "REPLACE"))){
                            System.err.format("[Error] Execute SQL >>>\n %s  Failed\n", updateSql);

                        }
                    }
                }
                updatedList.add(item.getIndex());
            });

            System.out.format("[Info] Updated id in data source %s are %s\n\n", runner.getDBInfo(), updatedList.toString());

        }


        return null;
    }
}
