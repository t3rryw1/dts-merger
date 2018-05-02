package com.cozystay.datasource;

import com.aliyun.drc.clusterclient.message.ClusterMessage;
import com.cozystay.db.SimpleDBWriterImpl;
import com.cozystay.db.Writer;
import com.cozystay.db.schema.SchemaLoader;
import com.cozystay.db.schema.SchemaRuleCollection;
import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;
import com.cozystay.model.SyncTaskBuilder;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public abstract class BinLogDataSourceImpl implements DataSource {
    private final SchemaRuleCollection schemaRuleCollection;
    private final Writer writer;
    private final BinaryLogClient client;

    private String subscribeInstanceID;
    private SchemaLoader schemaLoader;
    private Jedis redisClient;

    protected BinLogDataSourceImpl(Properties prop, String prefix) throws Exception {
        String dbAddress,
                subscribeInstanceID,
                dbUser,
                dbPassword,
                redisHost,
                redisPassword;
        int dbPort, redisPort;
        if ((dbAddress = prop.getProperty(prefix + ".dbAddress")) == null) {
            throw new ParseException(prefix + ".dbAddress", 1);
        }
        if ((dbUser = prop.getProperty(prefix + ".dbUser")) == null) {
            throw new ParseException(prefix + ".dbUser", 2);
        }
        if ((dbPassword = prop.getProperty(prefix + ".dbPassword")) == null) {
            throw new ParseException(prefix + ".dbPassword", 3);
        }
        if ((dbPort = Integer.valueOf(prop.getProperty(prefix + ".dbPort"))) <= 0) {
            throw new ParseException(prefix + ".dbPort", 4);
        }

        if ((subscribeInstanceID = prop.getProperty(prefix + ".subscribeInstanceID")) == null) {
            throw new ParseException(prefix + ".subscribeInstanceID", 5);
        }

        if ((redisHost = prop.getProperty("redis.host")) == null) {
            throw new ParseException("redis.host", 6);
        }

        if ((redisPort = Integer.valueOf(prop.getProperty("redis.port"))) <= 0) {
            throw new ParseException("redis.port", 7);
        }
        if ((redisPassword = prop.getProperty("redis.password")) == null) {
            throw new ParseException("redis.password", 8);
        }


        this.subscribeInstanceID = subscribeInstanceID;
        this.schemaRuleCollection = SchemaRuleCollection.loadRules(prop);
        this.schemaLoader = new SchemaLoader(dbAddress, dbPort, dbUser, dbPassword);
        JedisPool jedisPool = new JedisPool(redisHost, redisPort);

        redisClient = jedisPool.getResource();
        if (!redisPassword.equals("")) {
            redisClient.auth(redisPassword);

        }

        System.out.printf("Start BinLogDataSource using config: %s:%d, instance %s %n",
                dbAddress,
                dbPort,
                subscribeInstanceID);
        writer = new SimpleDBWriterImpl(dbAddress, dbPort, dbUser, dbPassword);

        client = new BinaryLogClient(dbAddress, dbPort, dbUser, dbPassword);
        SyncTaskBuilder.addSource(subscribeInstanceID);

    }

    @Override
    public String getName() {
        return this.subscribeInstanceID;
    }

    @Override
    public void writeDB(SyncOperation operation) {
        this.writer.write(operation);
    }

    @Override
    public void start() {
        schemaLoader.loadDBSchema(schemaRuleCollection);
        EventDeserializer eventDeserializer = new EventDeserializer();
        client.registerEventListener(new BinaryLogClient.EventListener() {
            private String currentTable;
            private String currentDB;

            @Override
            public void onEvent(Event event) {


                switch (event.getHeader().getEventType()) {
                    case TABLE_MAP: {
                        TableMapEventData data = event.getData();
                        System.out.printf("%s, %s %n",
                                data.getDatabase(),
                                data.getTable());
                        currentTable = data.getTable();
                        currentDB = data.getDatabase();
                        break;

                    }
                    case UPDATE_ROWS:
                    case DELETE_ROWS:
                    case WRITE_ROWS:
                        SyncTask task = (new BinLogEventParser()).parseTask(event,
                                schemaLoader, subscribeInstanceID,
                                currentTable,
                                currentDB);
                        if (task == null) {
                            break;
                        }
                        if (schemaRuleCollection.filter(task.getOperations().get(0))) {
                            break;
                        }
                        consumeData(task);

                        break;

                    default:

                }


            }


        });

        client.registerLifecycleListener(new BinaryLogClient.LifecycleListener() {
            private void saveBinlog(BinaryLogClient binaryLogClient){
                Long binlogPos = binaryLogClient.getBinlogPosition();
                System.out.printf(" %s.%d%n", binaryLogClient.getBinlogFilename(), binlogPos);
                redisClient.set("binlogFile-" + subscribeInstanceID, binaryLogClient.getBinlogFilename());
                redisClient.set("binlogPosition-" + subscribeInstanceID, String.valueOf(binlogPos));

            }

            @Override
            public void onConnect(BinaryLogClient binaryLogClient) {
                System.out.printf("bin log position onConnect: ");
                //binlog location logic
                saveBinlog(binaryLogClient);


            }

            @Override
            public void onCommunicationFailure(BinaryLogClient binaryLogClient, Exception e) {
                System.out.printf("bin log position onCommunicationFailure: ");
                //binlog location logic
                saveBinlog(binaryLogClient);

            }

            @Override
            public void onEventDeserializationFailure(BinaryLogClient binaryLogClient, Exception e) {
                System.out.printf("bin log position onEventDeserializationFailure: ");
                //binlog location logic
                saveBinlog(binaryLogClient);

            }

            @Override
            public void onDisconnect(BinaryLogClient binaryLogClient) {

                System.out.printf("bin log position onDisconnect: ");
                //binlog location logic
                saveBinlog(binaryLogClient);

            }
        });
        try {
            client.setEventDeserializer(eventDeserializer);
            client.setConnectTimeout(10000);
            client.setKeepAliveConnectTimeout(10000);
            String binlogFileName, binlogPosition;
            if (
                    (binlogFileName = redisClient.get("binlogFile-" + this.subscribeInstanceID))
                            !=
                            null
                            &&
                            (binlogPosition = redisClient.get("binlogPosition-" + this.subscribeInstanceID))
                                    !=
                                    null) {
                client.setBinlogFilename(binlogFileName);
                client.setBinlogPosition(Long.valueOf(binlogPosition));
            }
            client.connect(10000);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }


    @Override
    public boolean shouldFilterMessage(ClusterMessage message) {
        return false;
    }

    @Override
    public void stop() {
        try {
            client.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
