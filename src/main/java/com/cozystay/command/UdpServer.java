package com.cozystay.command;

import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncTask;
import com.cozystay.structure.TaskPool;
import com.cozystay.structure.TaskQueue;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.cozystay.command.MessageCategories.*;

import java.io.IOException;

public class UdpServer extends Thread  {
    private final int port;
    private final TaskPool primaryPool;
    private final TaskPool secondaryPool;
    private final TaskPool donePool;
    private final TaskPool failedPool;
    private final TaskQueue todoQueue;

    public UdpServer(int port, TaskPool primaryPool,
                     TaskPool secondaryPool,
                     TaskPool donePool,
                     TaskPool failedPool,
                     TaskQueue todoQueue) {
        this.port = port;
        this.primaryPool = primaryPool;
        this.secondaryPool = secondaryPool;
        this.donePool = donePool;
        this.failedPool = failedPool;
        this.todoQueue = todoQueue;
    }

    private Task ProcessTask (Task response, SyncTask task) {
        switch (response.operationType) {
            case VIEW:
                response.database = task.getDatabase();
                response.table = task.getTable();
                response.operations = task.getOperations();
                response.success = true;
            case REMOVE:
                primaryPool.remove(task);
                secondaryPool.remove(task);
                donePool.remove(task);
        }

        return response;
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

                if (object instanceof Status) {
                    Status response = (Status) object;
                    response.FinishedTaskNumInHour = null;
                    response.PrimayQueueTaskNum = primaryPool.size();
                    response.SecondQueueTaskNum = secondaryPool.size();
                    response.DonePoolTaskNum = donePool.size();
                    response.FailedTaskNum = failedPool.size();
                    response.success = true;

                    connection.sendUDP(response);

                    failedPool.removeAll();
                }

                if (object instanceof Task) {
                    Task response = (Task) object;

                    SyncTask primaryTask = primaryPool.get(response.taskId);
                    if (primaryTask != null) {
                        connection.sendUDP(ProcessTask(response, primaryTask));
                        return;
                    }

                    SyncTask secondaryTask = secondaryPool.get(response.taskId);
                    if (secondaryTask != null) {
                        connection.sendUDP(ProcessTask(response, secondaryTask));
                        return;
                    }

                    SyncTask donePoolTask = donePool.get(response.taskId);
                    if (donePoolTask != null) {
                        connection.sendUDP(ProcessTask(response, donePoolTask));
                        return;
                    }

                    response.success = false;
                    response.message = "not found task: " + response.taskId;
                    connection.sendUDP(response);
                    return;
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
