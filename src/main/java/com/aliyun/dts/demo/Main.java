package com.aliyun.dts.demo;

import com.aliyun.drc.clusterclient.ClusterClient;
import com.aliyun.drc.clusterclient.ClusterListener;
import com.aliyun.drc.clusterclient.DefaultClusterClient;
import com.aliyun.drc.clusterclient.RegionContext;
import com.aliyun.drc.clusterclient.message.ClusterMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Description:
 * This project demo shows how to simply subscribe and consume data with DTS SDK
 */
public class Main {

    private static Logger logger = LoggerFactory.getLogger(Main.class);


    // 用户需要替换自己使用的accessKey, accessSecret, subscribeInstanceID
    private static String accessKey = "";
    private static String accessSecret = "";
    private static String subscribeInstanceID = "";

    // 命令行输入参数
    private static void parsePromptArgs(String args[]) {
        for (int i = 0; i < args.length; i++) {
            String prompt = args[i++];
            if (i >= args.length) {
                break;
            } else if (prompt.equals("--accessKey")) {
                accessKey = args[i];
            } else if (prompt.equals("--accessSecret")) {
                accessSecret = args[i];
            } else if (prompt.equals("--subscribeInstanceID")) {
                subscribeInstanceID = args[i];
            } else {
                System.err.println("Usage: java -jar demo.jar --accessKey accessKey --accessSecret accessSecret --subscribeInstanceID subscribeInstanceID");
                System.exit(1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        parsePromptArgs(args);


        logger.info("start to demo...");
        /* 创建一个context */
        RegionContext context = new RegionContext();
        context.setUsePublicIp(true);
        context.setAccessKey(accessKey);
        context.setSecret(accessSecret);

        /* 创建集群消费client */
        final ClusterClient client = new DefaultClusterClient(context);
        /* 创建集群消费listener */
        ClusterListener listener = new ClusterListener() {
            @Override
            public void notify(List<ClusterMessage> messages) throws Exception {
                for (ClusterMessage message : messages) {

                    if (message.getRecord().getTablename() == null) {
                        message.ackAsConsumed();
                        continue;
                    }
                    if (message.getRecord().getDbname() == null) {
                        message.ackAsConsumed();
                        continue;
                    }

                    if (message.getRecord().getTablename().equals("calendar")) {
                        message.ackAsConsumed();
                        continue;
                    }

                    /* 可打印数据 */
                    logger.error(message.getRecord().getDbname() + ":"
                            + message.getRecord().getTablename() + ":"
                            + message.getRecord().getOpt() + ":"
                            + message.getRecord().getTimestamp() + ":"
                            + message.getRecord());


                    // ackAsConsumed必须调用
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
        client.askForGUID(subscribeInstanceID);
        /* 启动后台线程 */
        client.start();
    }
}
