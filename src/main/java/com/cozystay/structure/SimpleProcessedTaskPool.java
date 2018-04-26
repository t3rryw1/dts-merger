package com.cozystay.structure;

import com.cozystay.model.SyncOperationImpl;
import com.cozystay.model.SyncTask;
import com.cozystay.model.SyncTaskImpl;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.io.ByteArrayOutputStream;
import java.util.Date;
import java.util.Set;

public class SimpleProcessedTaskPool implements ProcessedTaskPool {
    //    private ConcurrentHashMap<String, SyncTask> taskMap;
//    private ConcurrentLinkedQueue<String> idQueue;
    private Jedis redisClient;
    private Kryo kryo;


    public SimpleProcessedTaskPool(String host, int port, String password) {
        redisClient = new Jedis(host, port);
        redisClient.auth(password);
        kryo = new Kryo();
        kryo.register(SyncTaskImpl.class);
        kryo.register(SyncOperationImpl.class);
//        taskMap = new ConcurrentHashMap<>();
//        idQueue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void add(SyncTask task) {
        byte[] taskArray = encode(kryo, task);
        Transaction transaction = redisClient.multi();
        transaction.hset("cozy-data-hash".getBytes(), task.getId().getBytes(), taskArray);
        transaction.zadd("cozy-data-sort-set", new Date().getTime(), task.getId());
        transaction.exec();

//        taskMap.put(task.getId(), task);
//        idQueue.add(task.getId());
    }

    @Override
    public void remove(SyncTask task) {
        this.remove(task.getId());

    }

    @Override
    public void remove(String taskId) {
        Transaction transaction = redisClient.multi();
        transaction.hdel("cozy-data-hash".getBytes(), taskId.getBytes());
        transaction.zrem("cozy-data-sort-set", taskId);
        transaction.exec();
    }

    @Override
    public boolean hasTask(SyncTask task) {
        return redisClient.hexists("cozy-data-hash", task.getId());
//        return taskMap.containsKey(task.getId());
    }

    @Override
    public SyncTask poll() {
        Set<String> keySet = redisClient.zrange("cozy-data-sort-set", 0, 0);
        if (keySet.isEmpty()) {
            return null;
        }
        String key = (String) keySet.toArray()[0];
        SyncTask task = get(key);
        remove(key);
        return task;

    }

    @Override
    public SyncTask get(String taskId) {
        byte[] taskBytes = redisClient.hget("cozy-data-hash".getBytes(), taskId.getBytes());
        return decode(this.kryo, taskBytes);
    }

    private static byte[] encode(Kryo kryo, Object obj) {
        ByteArrayOutputStream objStream = new ByteArrayOutputStream();
        Output objOutput = new Output(objStream);
        kryo.writeClassAndObject(objOutput, obj);
        objOutput.close();
        return objStream.toByteArray();
    }

    private static <T> T decode(Kryo kryo, byte[] bytes) {
        return (T) kryo.readClassAndObject(new Input(bytes));
    }
}
