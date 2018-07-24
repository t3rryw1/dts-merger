package com.cozystay.command;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.EndPoint;
import com.cozystay.model.SyncOperation;

import java.util.List;

public class MessageCategories {
    static public void register (EndPoint endPoint) {
        Kryo kryo = endPoint.getKryo();
        kryo.register(SimplifyMessage.class);
    }

    static public class SimplifyMessage {
        public String message;
    }

    static public class GlobalStatus {
        public int FinishedTaskNumInHour;
        public int PrimayQueueTaskNum;
        public int SecondQueueTaskNum;
        public int FailedTaskNum;
        public String message;
        public boolean success;
    }

    static public class TaskDetail {
        public String taskId;
        public String database;
        public String table;
        public List<SyncOperation> operations;
        public String message;
        public boolean success;
    }

    static public class TaskReset {
        public String taskId;
        public String message;
        public boolean success;
    }

    static public class TaskRemove {
        public String taskId;
        public String message;
        public boolean success;
    }
}