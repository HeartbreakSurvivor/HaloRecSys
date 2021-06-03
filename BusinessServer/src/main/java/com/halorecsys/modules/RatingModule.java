package com.halorecsys.modules;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.halorecsys.dataloader.DataLoader;
import com.halorecsys.dataloader.Rating;
import com.halorecsys.utils.Config;
import com.halorecsys.utils.MongoDBClient;
import com.halorecsys.utils.RedisClient;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.apache.log4j.Logger;
import org.bson.Document;
import redis.clients.jedis.Jedis;

/**
 * @program: HaloRecSys
 * @description: operations of user ratings
 * @author: HaloZhang
 * @create: 2021-05-25 14:24
 **/
public class RatingModule {
    static MongoCollection<Document> ratingCollection = MongoDBClient.getInstance().getDatabase(Config.DATABASE_NAME)
            .getCollection(Config.MONGODB_RATING_COLLECTION);
    private static Logger logger = Logger.getLogger(RatingModule.class.getName());

    public static boolean movieRating(Rating rating) {
        updateRedis(rating);
        System.out.print("=========movieRating=========");
        logger.info(Config.MOVIE_RATING_PREFIX + ":" + rating.getUserName() +"|"+ rating.getMovieId() +"|"+ rating.getScore() +"|"+ rating.getTimestamp());
        if (isRatingExist(rating)) {
            // update rating records
            return updateRating(rating);
        } else {
            // insert a new one
            ratingCollection.insertOne(new Document("uid", rating.getUserName()).append("mid", rating.getMovieId()).append("score", rating.getScore()).append("timestamp", rating.getTimestamp()));
            return true;
        }
    }

    static void updateRedis(Rating rating) {
        // get userId by username
        int userId = DataLoader.getInstance().getUserByName(rating.getUserName()).getUserId();

        Jedis jedis = RedisClient.getInstance();

        if (jedis.exists("uid:" + rating.getUserName()) && jedis.llen("uid:" + rating.getUserName()) >= Config.REDIS_MOVIE_RATING_QUEUE_SIZE) {
            jedis.rpop("uid:" + rating.getUserName());
        }
        // write to redis for spark streaming use
        jedis.lpush("uid:" + rating.getUserName(), rating.getMovieId() + ":" + rating.getScore());
    }

    static boolean updateRating(Rating rating) {
        BasicDBObject query = new BasicDBObject();
        query.append("uid", rating.getUserName());
        query.append("mid", rating.getMovieId());

        ratingCollection.updateOne(query,
                new Document().append("$set", new Document("score", rating.getScore()).append("timestamp", rating.getTimestamp())));

        BasicDBObject newDocument = new BasicDBObject();
        newDocument.put("score", rating.getScore());
        newDocument.put("timestamp", rating.getTimestamp());

        BasicDBObject updateObject = new BasicDBObject();
        updateObject.put("$set", newDocument);

        ratingCollection.updateOne(query, updateObject);
        return true;
    }

    static boolean isRatingExist(Rating rating) {
        BasicDBObject basicDBObject = new BasicDBObject();
        basicDBObject.append("uid", rating.getUserName());
        basicDBObject.append("mid", rating.getMovieId());
        FindIterable<Document> documents = ratingCollection.find(basicDBObject);
        return documents.first() != null;
    }

}
