package com.halorecsys.dataloader;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * @program: HaloRecSys
 * @description: The definition of users of halo recommendation system
 * @author: HaloZhang
 * @create: 2021-05-06 19:13
 **/
public class User {
    int userId;
    double averageRating = 0;
    double highestRating = 0;
    double lowestRating = 5.0;
    int ratingCount = 0;

    // 相似用户列表
    List<Integer> similarUsers;

    // @JsonSerialize(using = RatingListSerializer.class)
    List<Rating> ratings;

    //embedding of the movie
    @JsonIgnore
    Embedding emb;

    public User(){
        this.ratings = new ArrayList<>();
        this.similarUsers = new ArrayList<>();
        this.emb = null;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public List<Rating> getRatings() {
        return ratings;
    }

    public void setRatings(List<Rating> ratings) {
        this.ratings = ratings;
    }

    public void addRating(Rating rating) {
        this.ratings.add(rating);
        this.averageRating = (this.averageRating * ratingCount + rating.getScore()) / (ratingCount + 1);
        if (rating.getScore() > highestRating) {
            highestRating = rating.getScore();
        }
        if (rating.getScore() < lowestRating) {
            lowestRating = rating.getScore();
        }
        ratingCount++;
    }

    public void addSimilarUser(int uid) { this.similarUsers.add(uid); }

    public double getAverageRating() {
        return averageRating;
    }

    public void setAverageRating(double averageRating) {
        this.averageRating = averageRating;
    }

    public double getHighestRating() {
        return highestRating;
    }

    public void setHighestRating(double highestRating) {
        this.highestRating = highestRating;
    }

    public double getLowestRating() {
        return lowestRating;
    }

    public void setLowestRating(double lowestRating) {
        this.lowestRating = lowestRating;
    }

    public int getRatingCount() {
        return ratingCount;
    }

    public void setRatingCount(int ratingCount) {
        this.ratingCount = ratingCount;
    }

    public Embedding getEmb() { return emb; }

    public void setEmb(Embedding emb) { this.emb = emb; }
}
