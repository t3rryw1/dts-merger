package com.cozystay.dts;

import com.aliyun.drc.clusterclient.ClusterClient;
import com.aliyun.drc.clusterclient.ClusterListener;
import com.aliyun.drc.clusterclient.DefaultClusterClient;
import com.aliyun.drc.clusterclient.RegionContext;
import com.aliyun.drc.clusterclient.message.ClusterMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractDataConsumer implements DataConsumer {
    private final RegionContext context;
    private final String subscribeInstanceID;
    private static Logger logger = LoggerFactory.getLogger(AbstractDataConsumer.class);


    public AbstractDataConsumer(String accessKey, String accessSecret, String subscribeInstanceID) {
        context = new RegionContext();
        context.setUsePublicIp(true);
        context.setAccessKey(accessKey);
        context.setSecret(accessSecret);
        this.subscribeInstanceID = subscribeInstanceID;

    }

    @Override
    public void start() {
 /* 创建集群消费client */
        final ClusterClient client = new DefaultClusterClient(context);
        /* 创建集群消费listener */
        ClusterListener listener = new ClusterListener() {
            @Override
            public void notify(List<ClusterMessage> messages) throws Exception {
                for (ClusterMessage message : messages) {
                    if (shouldConsume(message.getRecord())) {
                        consumeData(message.getRecord());
                    }
                    message.ackAsConsumed();


//                    if (message.getRecord().getTablename() == null) {
//                        message.ackAsConsumed();
//                        continue;
//                    }
//                    if (message.getRecord().getDbname() == null) {
//                        message.ackAsConsumed();
//                        continue;
//                    }
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
//                    // ackAsConsumed必须调用

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
}
