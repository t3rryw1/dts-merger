package com.cozystay.structure;

import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncOperationImpl;
import com.cozystay.model.SyncTask;
import com.cozystay.model.SyncTaskImpl;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.Set;

public class RedisProcessedTaskPool implements ProcessedTaskPool {
    public static final String DATA_NOTIFY_HASH_KEY = "cozy-notify-hash";
    public static final String DATA_NOTIFY_SET_KEY = "cozy-notify-sort-set";
    public static final String DATA_PRIMARY_HASH_KEY = "cozy-data-hash";
    public static final String DATA_PRIMARY_SET_KEY = "cozy-data-sort-set";
    public static final String DATA_SECONDARY_HASH_KEY = "cozy-sec-data-hash";
    public static final String DATA_SECONDARY_SET_KEY = "cozy-sec-data-sort-set";

    private Jedis redisClient;
    private Kryo kryo;

    private String hashKeyName;

    private String setKeyName;

    public RedisProcessedTaskPool(String host,
                                  int port,
                                  String password,
                                  String hashKeyName,
                                  String setKeyName) {
        JedisPool jedisPool = new JedisPool(host, port);

        redisClient = jedisPool.getResource();
        if(!password.equals("")){
            redisClient.auth(password);

        }
        kryo = new Kryo();
        kryo.register(SyncTaskImpl.class);
        kryo.register(SyncOperationImpl.class);
        kryo.register(ArrayList.class);
        kryo.register(SyncOperation.SyncItem.class);
        kryo.register(LinkedList.class);
        this.hashKeyName = hashKeyName;
        this.setKeyName = setKeyName;
    }

    @Override
    public void add(SyncTask task) {
        byte[] taskArray = encode(kryo, task);
        Transaction transaction = redisClient.multi();
        transaction.hset(this.hashKeyName.getBytes(), task.getId().getBytes(), taskArray);
        transaction.zadd(this.setKeyName,
                new Date().getTime(),
                task.getId());
        transaction.exec();

    }

    @Override
    public void remove(SyncTask task) {
        this.remove(task.getId());

    }

    @Override
    public void remove(String taskId) {
        Transaction transaction = redisClient.multi();
        transaction.hdel(this.hashKeyName.getBytes(), taskId.getBytes());
        transaction.zrem(this.setKeyName, taskId);
        transaction.exec();
    }

    @Override
    public boolean hasTask(SyncTask task) {
        return redisClient.hexists(this.hashKeyName, task.getId());
    }

    @Override
    public synchronized SyncTask poll() {
        Set<String> keySet = redisClient.zrange(this.setKeyName, 0, 0);
        if (keySet.isEmpty()) {
            return null;
        }
        String key = (String) keySet.toArray()[0];
        SyncTask task = get(key);
        remove(key);
        return task;

    }

    @Override
    public SyncTask peek() {
        Set<String> keySet = redisClient.zrange(this.setKeyName, 0, 0);
        if (keySet.isEmpty()) {
            return null;
        }
        String key = (String) keySet.toArray()[0];
        return get(key);
    }

    @Override
    public synchronized SyncTask get(String taskId) {
        byte[] taskBytes = redisClient.hget(this.hashKeyName.getBytes(), taskId.getBytes());
        return decode(this.kryo, taskBytes);
    }

    @Override
    public void destroy() {
        if(redisClient!=null && redisClient.isConnected()){
            redisClient.disconnect();
        }
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
