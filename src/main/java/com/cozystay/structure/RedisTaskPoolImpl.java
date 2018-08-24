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

public class RedisTaskPoolImpl implements TaskPool {
    private static Logger logger = LoggerFactory.getLogger(RedisTaskPoolImpl.class);
    private final JedisPool jedisPool;

    private Kryo kryo;

    private String hashKeyName;

    private String setKeyName;

    public RedisTaskPoolImpl(String host,
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
        }catch (Exception ex){
            logger.error(ex.getMessage());
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
        }catch (Exception ex){
            logger.error(ex.getMessage());

        }
    }

    @Override
    public boolean hasTask(SyncTask task) {
        try (Jedis redisClient = jedisPool.getResource()) {

            return redisClient.hexists(this.hashKeyName, task.getId());
        }catch (Exception ex){
            logger.error(ex.getMessage());
            return false;
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
        }catch (Exception ex){
            logger.error(ex.getMessage());
            return null;
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
        }catch (Exception ex){
            logger.error(ex.getMessage());
            return null;
        }
    }

    @Override
    public Long size() {
        try (Jedis redisClient = jedisPool.getResource()) {
            return redisClient.zcard(this.setKeyName);
        }catch (Exception ex){
            logger.error(ex.getMessage());
            return null;
        }
    }

    @Override
    public void removeAll() {
        try (Jedis redisClient = jedisPool.getResource()) {
            redisClient.del(this.setKeyName);
        }catch (Exception ex){
            logger.error(ex.getMessage());
        }
    }

    @Override
    public synchronized SyncTask get(String taskId) {
        try (Jedis redisClient = jedisPool.getResource()) {

            byte[] taskBytes = redisClient.hget(this.hashKeyName.getBytes(), taskId.getBytes());
            return KryoEncodeHelper.decode(this.kryo, taskBytes, SyncTask.class);
        }catch (Exception ex){
            logger.error(ex.getMessage());
            return null;
        }
    }

    @Override
    public void destroy() {
        jedisPool.close();

    }


}
