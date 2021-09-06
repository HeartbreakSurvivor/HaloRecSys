package com.halorecsys.modules;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.halorecsys.dataloader.DataLoader;
import com.halorecsys.utils.Config;
import com.halorecsys.utils.MongoDBClient;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
            if (pwd.equals(password)) {
                return true;
            }
            return false;
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
        //TODO
        // 先获取当前用户的最大编号，然后加一，再写入
        // 根据 {id} 来进行降序排列，然后取出对应的id，再加一
        Document docs = userCollection.find().sort(Sorts.descending("userIdx")).first();
        int userId = docs.getInteger("userIdx") + 1;
        // write to mongoDB
        userCollection.insertOne(new Document("username", username).append("password", password).append("userIdx", userId));
        // write to memory
        DataLoader.getInstance().setUser(username, userId);
        return true;
    }

    public static void SetUserPres(String username, String[] genres) {
        List<String> prefGenres = Arrays.asList(genres);
        //update exist user data in mongoDB
        userCollection.findOneAndUpdate(eq("username", username), Updates.pushEach("preGenres", prefGenres));
        // write to memory
        DataLoader.getInstance().getUserByName(username).setPrefGenres(Arrays.asList(genres));
    }
}
