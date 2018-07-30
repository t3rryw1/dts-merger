package com.cozystay.command;

import com.cozystay.model.SyncTask;
import com.cozystay.structure.TaskPool;
import com.cozystay.structure.TaskQueue;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.cozystay.command.MessageCategories.*;

import java.io.IOException;
import java.util.*;

public class UdpServer extends Thread  {
    private Server server;
    private final int port;
    private final TaskPool primaryPool;
    private final TaskPool secondaryPool;
    private final TaskPool donePool;
    private final TaskPool failedPool;
    private final TaskQueue todoQueue;
    private LinkedList<TaskRecord> finishedTaskInHour;

    public UdpServer(int port, TaskPool primaryPool,
                     TaskPool secondaryPool,
                     TaskPool donePool,
                     TaskPool failedPool,
                     TaskQueue todoQueue) {
        this.server = new Server();
        this.port = port;
        this.primaryPool = primaryPool;
        this.secondaryPool = secondaryPool;
        this.donePool = donePool;
        this.failedPool = failedPool;
        this.todoQueue = todoQueue;
        this.finishedTaskInHour = new LinkedList<>();
    }

    class TaskRecord {
        private final SyncTask task;
        private final Long recordTime;

        TaskRecord(SyncTask task){
            this.task = task;
            this.recordTime = new Date().getTime();
        }
    }

    private static Long getOneHoursAgoTime () {
        Calendar cal = Calendar.getInstance ();
        cal.set(Calendar.HOUR , Calendar.HOUR - 1);
        return cal.getTime().getTime();
    }

    public void addRecord(SyncTask task) {
        Long hourAgoTime = getOneHoursAgoTime();
        TaskRecord record = new TaskRecord(task);

        finishedTaskInHour.add(record);

        while (finishedTaskInHour.getFirst().recordTime < hourAgoTime) {
            finishedTaskInHour.removeFirst();
        }
    }

    private Task ProcessTask (Task response, SyncTask task, TaskPool pool) {
        switch (response.operationType) {
            case VIEW:
                response.message = "view task: " + task.getId();
                response.database = task.getDatabase();
                response.table = task.getTable();
                response.operations = task.getOperations();
                response.success = true;
            case REMOVE:
                synchronized (pool) {
                    pool.remove(task);
                }
                response.message = "successfully removed task: " + task.getId();
                response.success = true;
        }

        return response;
    }

    public void stopServer() {
        this.server.stop();
    }

    public void run(){
        server.start();
        server.addListener(new Listener() {
            public void received (Connection connection, Object object) {
                if (object instanceof SimplifyMessage) {
                    SimplifyMessage response = (SimplifyMessage) object;
                    response.message = "return simplify message";
                    connection.sendUDP(response);
                }

                if (object instanceof Status) {
                    Status response = (Status) object;
                    response.FinishedTaskNumInHour = finishedTaskInHour.size();
                    response.PrimaryQueueTaskNum = primaryPool.size();
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
                        connection.sendUDP(ProcessTask(response, primaryTask, primaryPool));
                        return;
                    }

                    SyncTask secondaryTask = secondaryPool.get(response.taskId);
                    if (secondaryTask != null) {
                        connection.sendUDP(ProcessTask(response, secondaryTask, secondaryPool));
                        return;
                    }

                    SyncTask donePoolTask = donePool.get(response.taskId);
                    if (donePoolTask != null) {
                        connection.sendUDP(ProcessTask(response, donePoolTask, donePool));
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
