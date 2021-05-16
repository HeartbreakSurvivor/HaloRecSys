package com.halorecsys.modules;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.halorecsys.utils.Config;
import com.halorecsys.utils.MongoDBClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;

/**
 * @program: HaloRecSys
 * @description: related operations of user
 * @author: HaloZhang
 * @create: 2021-05-16 17:06
 **/
public class UserModule {
    static MongoCollection<Document> userCollection = MongoDBClient.getInstance().getDatabase(Config.DATABASE_NAME)
        .getCollection(Config.MONGODB_USER_COLLECTION);

    public static boolean CheckUserInfo(String username, String password) {
        if (null != userCollection) {
            Document docs = userCollection.find(eq("username", username)).first();
            if (null == docs || docs.isEmpty()) {
                return false;
            }
            String pwd = docs.getString("password");
            if (pwd != password) {
                return false;
            }
            return true;
        }
        return false;
    }

    public static boolean IsUserExist(String username) {
        if (null != userCollection) {
            Document docs = userCollection.find(eq("username", username)).first();
            if (null == docs || docs.isEmpty()) {
                return false;
            }
            return true;
        }
        return false;
    }

    public static boolean RegisterUser(String username, String password) {
        if (IsUserExist(username)) return false;
        userCollection.insertOne(new Document("username", username).append("password", password));
        return true;
    }


}
