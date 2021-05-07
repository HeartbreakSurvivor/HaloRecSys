package com.halorecsys.utils;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

/**
 * @program: HaloRecSys
 * @description: the connection helper of MongoDB
 * @author: HaloZhang
 * @create: 2021-05-06 19:46
 **/
public class MongoDBClient {
    // 定义MongoClient对象
    private static volatile MongoClient mongoClient;
    final static String MONGO_END_POINT = "localhost";
    final static int MONGO_DEFAULT_PORT = 27017;

    //singleton MongoDB Client
    public static MongoClient getInstance() {
        if (null == mongoClient) {
            synchronized (RedisClient.class) { //反射？
                if (null == mongoClient) {
                    try {
                        mongoClient = new MongoClient(MONGO_END_POINT, MONGO_DEFAULT_PORT);
                    }
                    catch (Exception e) {
                        System.err.println( e.getClass().getName() + ": " + e.getMessage() );
                        return null;
                    }
                }
            }
        }
        return mongoClient;
    }

    public MongoDatabase getDatabase(String databaseName) {
        MongoDatabase mongoDatabase = mongoClient.getDatabase("name");
        System.out.println("Connect to database successfully");
        return mongoDatabase;
    }
}
