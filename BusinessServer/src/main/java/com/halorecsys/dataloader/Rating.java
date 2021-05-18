package com.halorecsys.dataloader;

/**
 * @program: HaloRecSys
 * @description: The definition of Rating class
 * @author: HaloZhang
 * @create: 2021-05-06 19:08
 **/
public class Rating {
    private int movieId;
    private String userName;
    private double score;
    private long timestamp;

    public Rating(int mid, String username, double score, long ts) {
        this.movieId = mid;
        this.userName = username;
        this.score = score;
        this.timestamp = ts;
    }
    
    public int getMovieId() { return movieId; }

    public void setMovieId(int movieId) { this.movieId = movieId; }

    public String getUserName() { return this.userName; }

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
