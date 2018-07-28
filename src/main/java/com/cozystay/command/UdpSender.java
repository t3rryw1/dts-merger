package com.cozystay.command;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Listener.ThreadedListener;
import com.cozystay.command.MessageCategories.*;
import java.io.IOException;

public class UdpSender {
    private final Client client;

    private UdpSender(String address, int port) throws IOException {
        client = new Client();
        client.start();

        client.addListener(new ThreadedListener(new Listener() {
            public void connected (Connection connection) {}
            public void received (Connection connection, Object object) {

                if (object instanceof SimplifyMessage) {
                    SimplifyMessage response = (SimplifyMessage) object;
                    System.out.print(response.message);
                }

                if (object instanceof Status) {
                    Status response = (Status) object;
                    System.out.print(response.FinishedTaskNumInHour + "tasks finished in last hour \n");
                    System.out.print(response.PrimaryQueueTaskNum + "tasks in primary queue \n");
                    System.out.print(response.SecondQueueTaskNum + "tasks in secondary queue \n");
                    System.out.print(response.DonePoolTaskNum + "tasks in done queue are waiting to be removed \n");
                    System.out.print(response.FailedTaskNum + "tasks failed since last check \n");
                    System.out.print(response.success);
                }

                if (object instanceof Task) {
                    Task response = (Task) object;
                    System.out.print(response.message + "\n");
                    System.out.print("operation" + response.success + "\n");
                }

                client.stop();
            }

            public void disconnected (Connection connection) {
                System.exit(0);
            }
        }));

        client.connect(1000, address, port, port);
        MessageCategories.register(client);
    }

    public void requestGlobalStatus() {
        Status data = new Status();
        client.sendUDP(data);
    }

    public void requestTaskDetail(String taskId) {
        Task data = new Task();
        data.operationType = OperationType.VIEW;
        data.taskId = taskId;
        client.sendUDP(data);
    }

    public void requestTaskRemove(String taskId) {
        Task data = new Task();
        data.operationType = OperationType.REMOVE;
        data.taskId = taskId;
        client.sendUDP(data);
    }

    public static UdpSender start(String address, int port) throws IOException  {
        return new UdpSender(address, port);
    }
}