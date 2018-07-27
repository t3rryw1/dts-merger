package com.cozystay.command;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.EndPoint;
import com.cozystay.model.SyncOperation;

import java.util.List;

public class MessageCategories {
    enum OperationType {
        VIEW,
        REMOVE
    }

    static public void register (EndPoint endPoint) {
        Kryo kryo = endPoint.getKryo();
        kryo.register(SimplifyMessage.class);
        kryo.register(Status.class);
        kryo.register(Task.class);
        kryo.register(OperationType.class);
    }

    static public class SimplifyMessage {
        public String message;
    }

    static public class Status {
        public int FinishedTaskNumInHour;
        public Long PrimaryQueueTaskNum;
        public Long SecondQueueTaskNum;
        public Long DonePoolTaskNum;
        public Long FailedTaskNum;
        public boolean success;
    }

    static public class Task {
        public OperationType operationType;
        public String taskId;
        public String database;
        public String table;
        public List<SyncOperation> operations;
        public String message;
        public boolean success;
    }
}