package com.cozystay.command;

import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;
import com.cozystay.structure.RedisProcessedTaskPool;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.cozystay.command.MessageCategories.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UdpServer extends Thread  {
    private final int port;
    private final RedisProcessedTaskPool pool;

    public UdpServer(int port, RedisProcessedTaskPool pool) {
        this.port = port;
        this.pool = pool;
    }

    public void run(){
        Server server = new Server();
        server.start();
        server.addListener(new Listener() {
            public void received (Connection connection, Object object) {
                if (object instanceof SimplifyMessage) {
                    SimplifyMessage response = (SimplifyMessage) object;
                    response.message = "return test message";
                    connection.sendUDP(response);
                }

                if (object instanceof GlobalStatus) {
                    GlobalStatus response = (GlobalStatus) object;
                    //TODO: get following Fields: FinishedTaskNumInHour/PrimayQueueTaskNum/SecondQueueTaskNum/FailedTaskNum
                    connection.sendUDP(response);
                }

                if (object instanceof TaskDetail) {
                    TaskDetail response = (TaskDetail) object;
                    SyncTask task = pool.get(response.taskId);

                    if(task == null){
                        response.success = false;
                        response.message = "not found task: " + response.taskId;
                        connection.sendUDP(response);
                        return;
                    }

                    response.database = task.getDatabase();
                    response.table = task.getTable();
                    response.operations = task.getOperations();
                    response.success = true;
                    connection.sendUDP(response);
                }

                if (object instanceof TaskReset) {
                    TaskReset response = (TaskReset) object;
                    SyncTask task = pool.get(response.taskId);

                    if(task == null){
                        response.success = false;
                        response.message = "not found task: " + response.taskId;
                        connection.sendUDP(response);
                        return;
                    }

                    pool.remove(task);

                    for(SyncOperation operation :task.getOperations()) {
                        List<String> keys = new ArrayList<>(operation.getSyncStatus().keySet());
                        for (String key : keys) {
                            SyncOperation.SyncStatus status = operation.getSyncStatus().get(key);

                            if (status != SyncOperation.SyncStatus.COMPLETED) {
                                operation.updateStatus(key,  SyncOperation.SyncStatus.INIT);
                            }
                        }
                    }

                    pool.add(task);
                    response.success = true;
                    connection.sendUDP(response);
                }

                if (object instanceof TaskRemove) {
                    TaskRemove response = (TaskRemove) object;
                    SyncTask task = pool.get(response.taskId);

                    if(task == null){
                        response.success = false;
                        response.message = "not found task: " + response.taskId;
                        connection.sendUDP(response);
                        return;
                    }

                    pool.remove(task);
                    connection.sendUDP(response);
                }
                connection.close();
            }
        });

        MessageCategories.register(server);

        try {
            server.bind(port, port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
