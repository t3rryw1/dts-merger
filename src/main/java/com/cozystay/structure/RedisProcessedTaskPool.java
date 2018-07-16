package com.cozystay.structure;

import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncOperationImpl;
import com.cozystay.model.SyncTask;
import com.cozystay.model.SyncTaskImpl;
import com.esotericsoftware.kryo.Kryo;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.Set;

public class RedisProcessedTaskPool implements ProcessedTaskPool {
    private static Logger logger = LoggerFactory.getLogger(RedisProcessedTaskPool.class);
    public static final String DATA_NOTIFY_HASH_KEY = "cozy-notify-hash";
    public static final String DATA_NOTIFY_SET_KEY = "cozy-notify-sort-set";
    public static final String DATA_PRIMARY_HASH_KEY = "cozy-data-hash";
    public static final String DATA_PRIMARY_SET_KEY = "cozy-data-sort-set";
    public static final String DATA_SECONDARY_HASH_KEY = "cozy-sec-data-hash";
    public static final String DATA_SECONDARY_SET_KEY = "cozy-sec-data-sort-set";
    public static final String DATA_SEND_HASH_KEY = "cozy-send-data-hash";
    public static final String DATA_SEND_SET_KEY = "cozy-send-data-sort-set";
    public static final String DATA_QUEUE_KEY = "cozy-queue-key";
    private final JedisPool jedisPool;

    private Kryo kryo;

    private String hashKeyName;

    private String setKeyName;

    public RedisProcessedTaskPool(String host,
                                  int port,
                                  String password,
                                  String hashKeyName,
                                  String setKeyName) {
        jedisPool = new JedisPool(new GenericObjectPoolConfig(), host, port, 2000, password);

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
        try (Jedis redisClient = jedisPool.getResource()) {
            byte[] taskArray = KryoEncodeHelper.encode(kryo, task);
            Transaction transaction = redisClient.multi();
            transaction.hset(this.hashKeyName.getBytes(), task.getId().getBytes(), taskArray);
            transaction.zadd(this.setKeyName,
                    new Date().getTime(),
                    task.getId());
            transaction.exec();
        }
    }

    @Override
    public void remove(SyncTask task) {
        this.remove(task.getId());

    }

    @Override
    public void remove(String taskId) {
        try (Jedis redisClient = jedisPool.getResource()) {

            Transaction transaction = redisClient.multi();
            transaction.hdel(this.hashKeyName.getBytes(), taskId.getBytes());
            transaction.zrem(this.setKeyName, taskId);
            transaction.exec();
        }
    }

    @Override
    public boolean hasTask(SyncTask task) {
        try (Jedis redisClient = jedisPool.getResource()) {

            return redisClient.hexists(this.hashKeyName, task.getId());
        }
    }

    @Override
    public synchronized SyncTask poll() {
        try (Jedis redisClient = jedisPool.getResource()) {

            Set<String> keySet = redisClient.zrange(this.setKeyName, 0, 0);
            if (keySet.isEmpty()) {
                return null;
            }
            String key = keySet.iterator().next();
            SyncTask task;
            try {

                task = get(key);
                return task;

            } catch (Exception e) {
                logger.error(String.format("Error key is: %s", key));
                logger.error(e.getMessage());
                return null;
            } finally {
                remove(key);

            }
        }

    }

    @Override
    public SyncTask peek() {
        try (Jedis redisClient = jedisPool.getResource()) {

            Set<String> keySet = redisClient.zrange(this.setKeyName, 0, 0);
            if (keySet.isEmpty()) {
                return null;
            }
            String key = keySet.iterator().next();
            SyncTask task;
            try {

                task = get(key);
                return task;

            } catch (Exception e) {
                logger.error(String.format("Error key is: %s", key));
                logger.error(e.getMessage());
                remove(key);
                return null;
            }
        }
    }

    @Override
    public synchronized SyncTask get(String taskId) {
        try (Jedis redisClient = jedisPool.getResource()) {

            byte[] taskBytes = redisClient.hget(this.hashKeyName.getBytes(), taskId.getBytes());
            return KryoEncodeHelper.decode(this.kryo, taskBytes, SyncTask.class);
        }
    }

    @Override
    public void destroy() {
        jedisPool.close();

    }


}
