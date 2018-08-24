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

    enum PoolName {
        PRIMARY,
        SECONDARY,
        DONE,
        FAILED
    }

    static public void register (EndPoint endPoint) {
        Kryo kryo = endPoint.getKryo();
        kryo.register(DeleteQueue.class);
        kryo.register(Status.class);
        kryo.register(Task.class);
        kryo.register(OperationType.class);
    }

    static public class DeleteQueue {
        public PoolName deletePoolName;
        public String message;
        public boolean success;
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
        public PoolName currentPoolName;
        public String database;
        public String table;
        public List<SyncOperation> operations;
        public String message;
        public boolean success;
    }
}