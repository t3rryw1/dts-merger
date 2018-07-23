package com.cozystay.structure;

import com.cozystay.model.SyncOperation;
import com.cozystay.model.SyncOperationImpl;
import com.cozystay.model.SyncTask;
import com.cozystay.model.SyncTaskImpl;
import com.esotericsoftware.kryo.Kryo;
import jdk.internal.instrumentation.Logger;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.util.ArrayList;
import java.util.LinkedList;

public class RedisTaskQueueImpl implements TaskQueue {

    private final Kryo kryo;
    private final String queueKeyName;
    private final JedisPool jedisPool;
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(RedisTaskQueueImpl.class);

    public RedisTaskQueueImpl(String host,
                              int port,
                              String password,
                              String queueKeyName) {
        jedisPool = new JedisPool(new GenericObjectPoolConfig(), host, port, 2000, password);

        kryo = new Kryo();
        kryo.register(SyncTaskImpl.class);
        kryo.register(SyncOperationImpl.class);
        kryo.register(ArrayList.class);
        kryo.register(SyncOperation.SyncItem.class);
        kryo.register(LinkedList.class);
        this.queueKeyName = queueKeyName;
    }

    @Override
    public void push(SyncTask task) {
        try (Jedis redisClient = jedisPool.getResource()) {

            byte[] taskArray = KryoEncodeHelper.encode(kryo, task);
            Transaction transaction = redisClient.multi();
            transaction.rpush(this.queueKeyName.getBytes(), taskArray);
            transaction.exec();
        }catch (Exception ex){
            logger.error(ex.getMessage());

        }
    }

    @Override
    public SyncTask pop() {
        try (Jedis redisClient = jedisPool.getResource()) {

            if (redisClient.llen(this.queueKeyName.getBytes()).intValue() > 0) {
                byte[] response = redisClient.lpop(this.queueKeyName.getBytes());
                return KryoEncodeHelper.decode(kryo, response, SyncTask.class);
            } else {
                return null;
            }
        }catch (Exception ex){
            logger.error(ex.getMessage());
            return null;
        }
    }

    @Override
    public int size() {
        try (Jedis redisClient = jedisPool.getResource()) {

            return redisClient.llen(this.queueKeyName.getBytes()).intValue();
        }catch (Exception ex){
            logger.error(ex.getMessage());
            return 0;

        }
    }


    @Override
    public void destroy() {
        jedisPool.close();
    }


}
