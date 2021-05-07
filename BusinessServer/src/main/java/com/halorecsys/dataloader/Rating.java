package com.halorecsys.dataloader;

/**
 * @program: HaloRecSys
 * @description: The definition of Rating class
 * @author: HaloZhang
 * @create: 2021-05-06 19:08
 **/
public class Rating {
    private int movieId;
    private int userId;
    private double score;
    private long timestamp;

    public Rating(int mid, int uid, double score, long ts) {
        this.movieId = mid;
        this.userId = uid;
        this.score = score;
        this.timestamp = timestamp;
    }
    
    public int getMovieId() { return movieId; }

    public void setMovieId(int movieId) { this.movieId = movieId; }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public double getScore() { return score; }

    public void setScore(float score) {
        this.score = score;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
