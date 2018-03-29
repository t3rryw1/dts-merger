package com.cozystay.dts;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.drc.client.message.DataMessage;
import com.aliyun.drc.clusterclient.ClusterClient;
import com.aliyun.drc.clusterclient.ClusterListener;
import com.aliyun.drc.clusterclient.DefaultClusterClient;
import com.aliyun.drc.clusterclient.RegionContext;
import com.aliyun.drc.clusterclient.message.ClusterMessage;
import com.cozystay.db.SimpleDBWriterImpl;
import com.cozystay.db.Writer;
import com.cozystay.model.SyncTask;
import com.cozystay.model.SyncTaskImpl;
import com.cozystay.model.SyncTaskBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.List;
import java.util.Properties;

public abstract class AbstractDataSourceImpl implements DataSource {
    private final String subscribeInstanceID;
    private static Logger logger = LoggerFactory.getLogger(AbstractDataSourceImpl.class);
    private final ClusterClient client;
    private final Writer writer;


    public AbstractDataSourceImpl(Properties prop, String prefix) throws Exception {


        String dbAddress, accessKey, accessSecret, subscribeInstanceID, dbUser, dbPassword;
        int dbPort;
        if ((dbAddress = prop.getProperty(prefix + ".dbAddress")) == null) {
            throw new ParseException(prefix + ".dbAddress",1);
        }
        if ((dbUser = prop.getProperty(prefix + ".dbUser")) == null) {
            throw new ParseException(prefix + ".dbAddress",1);
        }
        if ((dbPassword = prop.getProperty(prefix + ".dbPassword")) == null) {
            throw new ParseException(prefix + ".dbAddress",1);
        }
        if ((dbPort = Integer.valueOf(prop.getProperty(prefix + ".dbPort"))) <= 0) {
            throw new ParseException(prefix + ".dbAddress",1);
        }

        if ((accessKey = prop.getProperty(prefix + ".accessKey")) == null) {
            throw new ParseException(prefix + ".dbAddress",1);
        }
        if ((accessSecret = prop.getProperty(prefix + ".accessSecret")) == null) {
            throw new ParseException(prefix + ".dbAddress",1);
        }
        if ((subscribeInstanceID = prop.getProperty(prefix + ".subscribeInstanceID")) == null) {
            throw new ParseException(prefix + ".dbAddress",1);
        }


        System.out.println("Starting DataSource using config: "
                + dbAddress + ":" + dbPort
                + accessKey + " "
                + accessSecret + " "
                + subscribeInstanceID);
        writer = new SimpleDBWriterImpl(dbAddress, dbPort, dbUser, dbPassword);
        RegionContext context = new RegionContext();
        context.setUsePublicIp(true);
        context.setAccessKey(accessKey);
        context.setSecret(accessSecret);
        this.subscribeInstanceID = subscribeInstanceID;
        /* 创建集群消费client */
        client = new DefaultClusterClient(context);

    }

    @Override
    public void writeDB(SyncTask task) {
        this.writer.write(task);
    }

    @Override
    public String getName() {
        return this.subscribeInstanceID;
    }

    @Override
    public void start() {
        /* 创建集群消费listener */
        ClusterListener listener = new ClusterListener() {
            @Override
            public void notify(List<ClusterMessage> messages) {
                for (ClusterMessage message : messages) {
                    if (shouldFilterMessage(message)) {
                        message.ackAsConsumed();
                        continue;
                    }
                    SyncTask task = SyncTaskBuilder.build(message, subscribeInstanceID);
                    if(task!=null){
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


    public boolean shouldFilterMessage(ClusterMessage message) {
        //TODO: filter out useless messages


        DataMessage.Record record = message.getRecord();

        if (record.getTablename() == null) {
            return true;
        }
        if (record.getDbname() == null) {
            return true;
        }

        System.out.println("Record Op type:" + record.getOpt().toString());
        switch (record.getOpt()) {
            case INSERT: // 数据插入
            case UPDATE:// 数据更新
            case REPLACE:// replace操作
            case DELETE:// 数据删除
                return false;
            default:
                return true;
        }
        //TODO:  if nothing changes, abandon
        //TODO:  if only timestamp changes, abandon the message
        //TODO: if not a db commit message abandon
        //
//
//                    if (message.getRecord().getTablename().equals("calendar")) {
//                        message.ackAsConsumed();
//                        continue;
//                    }
//
//                    /* 可打印数据 */
//                    logger.error(message.getRecord().getDbname() + ":"
//                            + message.getRecord().getTablename() + ":"
//                            + message.getRecord().getOpt() + ":"
//                            + message.getRecord().getTimestamp() + ":"
//                            + message.getRecord());
//
//
    }



}
