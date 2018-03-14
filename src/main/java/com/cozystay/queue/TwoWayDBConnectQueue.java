package com.cozystay.queue;

import com.aliyun.drc.client.message.DataMessage;

import java.sql.Connection;
import java.sql.DriverManager;

public class TwoWayDBConnectQueue extends WorkerQueueImpl {

    TwoWayDBConnectQueue(String dbConnectorOne, String dbConnectorTwo){
        super();
//        try {
//            Class.forName("com.mysql.jdbc.Driver");
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }

        try {
            Connection connectOne = DriverManager
                    .getConnection(dbConnectorOne);
            Connection connectTwo = DriverManager
                    .getConnection(dbConnectorTwo);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @Override
    protected boolean allowToAdd(DataMessage.Record newRecord) {
        return false;
    }

    @Override
    protected void work(DataMessage.Record toProcess) {

    }

    public static void main(String[] args) throws Exception {
        System.out.print("main");
        TwoWayDBConnectQueue queue = new TwoWayDBConnectQueue("","");
        queue.start();
    }
}
