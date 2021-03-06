package com.halorecsys.dataloader;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @program: HaloRecSys
 * @description: The definition of Movie class
 * @author: HaloZhang
 * @create: 2021-05-06 19:05
 **/
public class Movie {
    int movieId;
    int movieIdx; // for embedding
    int releaseYear;
    String title;

    String imdbId;
    String tmdbId;
    // the genres of movie
    List<String> genres;
    //how many user rate the movie
    int ratingNumber;
    //average rating score
    double averageRating;

    //embedding of the movie
    @JsonIgnore
    Embedding emb;

    //all rating scores list
    @JsonIgnore
    List<Rating> ratings;

    final int TOP_RATING_SIZE = 10;

    @JsonSerialize(using = RatingListSerializer.class)
    List<Rating> topRatings;

    public Movie() {
        ratingNumber = 0;
        averageRating = 0;
        this.genres = new ArrayList<>();
        this.ratings = new ArrayList<>();
        this.topRatings = new LinkedList<>();
        this.emb = null;
    }

    public int getMovieId() {
        return movieId;
    }
    public void setMovieId(int movieId) { this.movieId = movieId; }

    public int getMovieIdx() { return movieIdx; }
    public void setMovieIdx(int movieIdx) { this.movieIdx = movieIdx; }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public int getReleaseYear() {
        return releaseYear;
    }

    public void setReleaseYear(int releaseYear) {
        this.releaseYear = releaseYear;
    }

    public List<String> getGenres() {
        return genres;
    }

    public void addGenre(String genre){
        this.genres.add(genre);
    }

    public void setGenres(List<String> genres) {
        this.genres = genres;
    }

    public List<Rating> getRatings() {
        return ratings;
    }

    public String getImdbId() {
        return imdbId;
    }

    public void setImdbId(String imdbId) {
        this.imdbId = imdbId;
    }

    public String getTmdbId() {
        return tmdbId;
    }

    public void setTmdbId(String tmdbId) {
        this.tmdbId = tmdbId;
    }

    public int getRatingNumber() {
        return ratingNumber;
    }

    public double getAverageRating() {
        return averageRating;
    }

    public Embedding getEmb() {
        return emb;
    }

    public void setEmb(Embedding emb) {
        this.emb = emb;
    }

    public void addRating(Rating rating) {
        // recalculate the average score
        averageRating = (averageRating * ratingNumber + rating.getScore()) / (ratingNumber+1);
        ratingNumber++;
        this.ratings.add(rating);
        addTopRating(rating);
    }

    public void addTopRating(Rating rating){
        // ????????????
        if (this.topRatings.isEmpty()){
            this.topRatings.add(rating);
        }else{
            int index = 0;
            for (Rating topRating : this.topRatings){
                if (topRating.getScore() >= rating.getScore()){
                    break;
                }
                index ++;
            }
            topRatings.add(index, rating);
            if (topRatings.size() > TOP_RATING_SIZE) {
                topRatings.remove(0);
            }
        }
    }

}
