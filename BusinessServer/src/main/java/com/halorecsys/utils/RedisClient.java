package com.halorecsys.utils;

import redis.clients.jedis.Jedis;

/**
 * @program: HaloRecSys
 * @description: the connection helper of redis
 * @author: HaloZhang
 * @create: 2021-05-06 19:27
 **/
public class RedisClient {
    // 定义redis对象
    private static volatile Jedis redisClient;
    final static String REDIS_END_POINT = "localhost";
    final static int REDIS_DEFAULT_PORT = 6379;

    //singleton Jedis
    public static Jedis getInstance() {
        if (null == redisClient) {
            synchronized (RedisClient.class) { //反射？
                if (null == redisClient) {
                    redisClient = new Jedis(REDIS_END_POINT, REDIS_DEFAULT_PORT);
                }
            }
        }
        return redisClient;
    }

}
