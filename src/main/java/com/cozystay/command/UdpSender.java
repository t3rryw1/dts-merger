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

                if (object instanceof GlobalStatus) {
                    GlobalStatus response = (GlobalStatus) object;

                }

                if (object instanceof TaskDetail) {
                    TaskDetail response = (TaskDetail) object;
                }

                if (object instanceof TaskReset) {
                    TaskReset response = (TaskReset) object;

                }

                if (object instanceof TaskRemove) {
                    TaskRemove response = (TaskRemove) object;

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
        GlobalStatus data = new GlobalStatus();
        client.sendUDP(data);
    }

    public void requestTaskDetail(String taskId) {
        TaskDetail data = new TaskDetail();
        data.taskId = taskId;
        client.sendUDP(data);
    }

    public void requestTaskReset(String taskId) {
        TaskReset data = new TaskReset();
        data.taskId = taskId;
        client.sendUDP(data);
    }

    public void requestTaskRemove(String taskId) {
        TaskRemove data = new TaskRemove();
        data.taskId = taskId;
        client.sendUDP(data);
    }

    public static UdpSender start(String address, int port) throws IOException  {
        return new UdpSender(address, port);
    }
}