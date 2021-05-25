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
    private int timestamp;

    public Rating(String username, int mid, double score, int ts) {
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

    public int getTimestamp() { return timestamp; }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }
}
