package com.cozystay.dts;

import com.aliyun.drc.clusterclient.ClusterClient;
import com.aliyun.drc.clusterclient.ClusterListener;
import com.aliyun.drc.clusterclient.DefaultClusterClient;
import com.aliyun.drc.clusterclient.RegionContext;
import com.aliyun.drc.clusterclient.message.ClusterMessage;
import com.cozystay.db.SimpleDBWriterImpl;
import com.cozystay.db.Writer;
import com.cozystay.model.SyncTask;
import com.cozystay.model.SyncTaskBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public abstract class AbstractDataSourceImpl implements DataSource {
    private final RegionContext context;
    private final String subscribeInstanceID;
    private static Logger logger = LoggerFactory.getLogger(AbstractDataSourceImpl.class);
    final ClusterClient client;
    final Writer writer;


    public AbstractDataSourceImpl(Properties prop, String prefix) throws Exception {


        String dbAddress, accessKey, accessSecret, subscribeInstanceID, dbUser, dbPassword;
        int dbPort;
        if ((dbAddress = prop.getProperty(prefix+ ".dbAddress")) == null) {
            throw new Exception(prefix+ ".dbAddress");
        }
        if ((dbUser = prop.getProperty(prefix + ".dbUser")) == null) {
            throw new Exception(prefix+ ".dbAddress");
        }
        if ((dbPassword = prop.getProperty(prefix + ".dbPassword")) == null) {
            throw new Exception(prefix+ ".dbAddress");
        }
        if ((dbPort = Integer.valueOf(prop.getProperty(prefix+ ".dbAddress"))) <= 0) {
            throw new Exception(prefix+ ".dbAddress");
        }

        if ((accessKey = prop.getProperty(prefix + ".accessKey")) == null) {
            throw new Exception(prefix+ ".dbAddress");
        }
        if ((accessSecret = prop.getProperty(prefix+ ".accessSecret")) == null) {
            throw new Exception(prefix+ ".dbAddress");
        }
        if ((subscribeInstanceID = prop.getProperty(prefix + ".subscribeInstanceID")) == null) {
            throw new Exception(prefix+ ".dbAddress");
        }


        System.out.println("Starting DataSource using config: "
                + dbAddress + ":" + dbPort
                + accessKey + " "
                + accessSecret + " "
                + subscribeInstanceID);
        writer = new SimpleDBWriterImpl(dbAddress,dbPort,dbUser,dbPassword);
        context = new RegionContext();
        context.setUsePublicIp(true);
        context.setAccessKey(accessKey);
        context.setSecret(accessSecret);
        this.subscribeInstanceID = subscribeInstanceID;
     /* 创建集群消费client */
        client = new DefaultClusterClient(context);

    }
    @Override
    public boolean shouldWriteDB(SyncTask task){
        //Note: basic rule is if the task is from the same source as current DataSource, skip it.
        return !task.source.equals(this.subscribeInstanceID);
    }

    @Override
    public void writeDB(SyncTask task) {
        this.writer.write(task);
    }

    @Override
    public void start() {
        /* 创建集群消费listener */
        ClusterListener listener = new ClusterListener() {
            @Override
            public void notify(List<ClusterMessage> messages) throws Exception {
                for (ClusterMessage message : messages) {
                    if (shouldFilterMessage(message)) {
                        message.ackAsConsumed();
                        continue;
                    }
                    SyncTask task = SyncTaskBuilder.build(message, subscribeInstanceID);

                    if (shouldConsume(task)) {
                        consumeData(task);
                    }
                    message.ackAsConsumed();

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





}
