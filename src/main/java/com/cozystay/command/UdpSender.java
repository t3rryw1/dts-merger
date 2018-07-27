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
                    System.out.print(response.FinishedTaskNumInHour);
                    System.out.print(response.PrimaryQueueTaskNum);
                    System.out.print(response.SecondQueueTaskNum);
                    System.out.print(response.DonePoolTaskNum);
                    System.out.print(response.FailedTaskNum);
                    System.out.print(response.success);
                }

                if (object instanceof Task) {
                    Task response = (Task) object;
                    System.out.print(response.message);
                    System.out.print(response.success);
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