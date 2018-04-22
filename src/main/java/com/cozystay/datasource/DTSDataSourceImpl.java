package com.cozystay.datasource;

import com.aliyun.drc.client.message.DataMessage;
import com.aliyun.drc.clusterclient.ClusterClient;
import com.aliyun.drc.clusterclient.ClusterListener;
import com.aliyun.drc.clusterclient.DefaultClusterClient;
import com.aliyun.drc.clusterclient.RegionContext;
import com.aliyun.drc.clusterclient.message.ClusterMessage;
import com.cozystay.db.SimpleDBWriterImpl;
import com.cozystay.db.Writer;
import com.cozystay.db.schema.SchemaRuleCollection;
import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.List;
import java.util.Properties;

public abstract class DTSDataSourceImpl implements DataSource {
    private final String subscribeInstanceID;
    private static Logger logger = LoggerFactory.getLogger(DTSDataSourceImpl.class);
    private final ClusterClient client;
    private final Writer writer;
    private final SchemaRuleCollection schemaRuleCollection;

    protected DTSDataSourceImpl(Properties prop, String prefix) throws Exception {


        String dbAddress, accessKey, accessSecret, subscribeInstanceID, dbUser, dbPassword;
        int dbPort;
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

        if ((accessKey = prop.getProperty(prefix + ".accessKey")) == null) {
            throw new ParseException(prefix + ".accessKey", 5);
        }
        if ((accessSecret = prop.getProperty(prefix + ".accessSecret")) == null) {
            throw new ParseException(prefix + ".accessSecret", 6);
        }
        if ((subscribeInstanceID = prop.getProperty(prefix + ".subscribeInstanceID")) == null) {
            throw new ParseException(prefix + ".subscribeInstanceID", 7);
        }

        this.schemaRuleCollection = SchemaRuleCollection.loadRules(prop);

        System.out.printf("Start DTSDataSource using config: %s:%d, access key: %s, instance id: %s%n",
                dbAddress,
                dbPort,
                accessKey,
                subscribeInstanceID);
        writer = new SimpleDBWriterImpl(dbAddress, dbPort, dbUser, dbPassword);
        RegionContext context = new RegionContext();
        context.setUsePublicIp(true);
        context.setAccessKey(accessKey);
        context.setSecret(accessSecret);
        this.subscribeInstanceID = subscribeInstanceID;

        client = new DefaultClusterClient(context);

    }

    @Override
    public void writeDB(SyncOperation operation) {
        this.writer.write(operation);
    }

    @Override
    public String getName() {
        return this.subscribeInstanceID;
    }

    @Override
    public void start() {
        /* 创建集群消费listener */
        System.out.printf("Start data source of instanceId : %s%n", subscribeInstanceID);
        ClusterListener listener = new ClusterListener() {
            @Override
            public void notify(List<ClusterMessage> messages) {
                for (ClusterMessage message : messages) {

                    try {
                        if (shouldFilterMessage(message)) {
                            continue;
                        }
                        SyncTask task = DTSMessageParser.parseMessage(message, subscribeInstanceID, schemaRuleCollection);
                        if (task != null) {
                            consumeData(task);
                        }
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    } finally {
                        System.out.printf("ACK message: %s/%s/%s%n",
                                message.getRecord().getDbname(),
                                message.getRecord().getTablename(),
                                message.getRecord().getId());
                        message.ackAsConsumed();

                    }

                }
            }

            @Override
            public void noException(Exception e) {
                logger.warn("", e);
            }
        };

        /* 设置监听者 */
        client.addConcurrentListener(listener);
        /* 设置请求的guid */
        try {
            client.askForGUID(subscribeInstanceID);
            client.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        /* 启动后台线程 */

    }

    public void stop() {
        try {
            client.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public boolean shouldFilterMessage(ClusterMessage message) {
        //filter out useless messages

        DataMessage.Record record = message.getRecord();

        if (record.getTablename() == null) {
            return true;
        }
        if (record.getDbname() == null) {
            return true;
        }

        System.out.printf("Record Op type: %s%n", record.getOpt().toString());
        switch (record.getOpt()) {
            case INSERT: // 数据插入
            case UPDATE:// 数据更新
            case REPLACE:// replace操作
            case DELETE:// 数据删除
                return false;
            default:
                //if not a db commit message abandon
                return true;
        }
    }


}
