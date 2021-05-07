package com.halorecsys.dataloader;

import com.halorecsys.utils.Config;
import com.halorecsys.utils.MongoDBClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @program: HaloRecSys
 * @description: Load all kinds of data from MongoDB to Memory
 * @author: HaloZhang
 * @create: 2021-05-06 19:17
 **/
public class DataLoader {
    // singleton instance
    private static DataLoader instance;

    // map Movies and users info to corresponding Id
    HashMap<Integer, Movie> movieMap;
    HashMap<Integer, User> userMap;

    // map movies to specific categories
    HashMap<String, List<Movie>> genresMap;

    // statistic recommendation
    // Rated most movies
    List<Movie> rateMostMovies;
    // highest average score movies
    List<Movie> highScoreMovies;
    // Rated most recently movies
    List<Movie> rateRecentlyMovies;
    // top 20 high average score in categories
    List<Movie> genresTopMovies;

    // Collaborative Filter Recommendation
    List<Movie> lfmRecMovies;

    // TF-IDF based Recommendation
    List<Movie> TFIDFRecMovies;

    private DataLoader() {
        this.movieMap = new HashMap<>();
        this.userMap = new HashMap<>();
        this.genresMap = new HashMap<>();

        this.rateMostMovies = new ArrayList<>();
        this.highScoreMovies = new ArrayList<>();
        this.rateRecentlyMovies = new ArrayList<>();
        this.genresTopMovies = new ArrayList<>();

        this.lfmRecMovies = new ArrayList<>();

        this.TFIDFRecMovies = new ArrayList<>();
        instance = this;
    }

    public static DataLoader getInstance() {
        if (null == instance) {
            synchronized (DataLoader.class) {
                if (null == instance) {
                    instance = new DataLoader();
                }
            }
        }
        return instance;
    }

    //parse release year
    private int parseReleaseYear(String rawTitle){
        if (null == rawTitle || rawTitle.trim().length() < 6){
            return -1;
        }else{
            // 提取 () 内的发行年份
            String yearString = rawTitle.trim().substring(rawTitle.length()-5, rawTitle.length()-1);
            try{
                return Integer.parseInt(yearString);
            }catch (NumberFormatException exception){
                return -1;
            }
        }
    }

    public void LoadMovieData(String dbName, String movieTable, String ratingTable, String linkTable) {
        MongoDatabase db = MongoDBClient.getInstance().getDatabase(dbName);

        int count = 0;
        // load movie data
        MongoCollection<Document> movies = db.getCollection(movieTable);
        for (Document doc : movies.find()) {
            // parse each movie
            int mid = doc.getInteger("mid");
            String name = doc.getString("name");
            String genres = doc.getString("genres");

            int releaseYear = parseReleaseYear(name);

            Movie m = new Movie();
            m.setMovieId(mid);
            if (releaseYear == -1){
                m.setTitle(name.trim());
            } else {
                m.setReleaseYear(releaseYear);
                m.setTitle(name.trim().substring(0, name.trim().length()-6).trim());
            }

            if (!genres.trim().isEmpty()) {
                String[] genreArray = genres.split("\\|");
                for (String g : genreArray) {
                    m.addGenre(g);
                    if (this.genresMap.containsKey(g)) {
                        genresMap.get(g).add(m);
                    } else {
                        genresMap.put(g, new ArrayList<>());
                    }
                }
            }
            count++;
            this.movieMap.put(mid, m);
        }
        System.out.println("Loading " + count + " movies completed. ");
        count = 0;

        // load rating data
        MongoCollection<Document> ratings = db.getCollection(ratingTable);
        for (Document doc : ratings.find()) {
            // parse each ratings data
            int mid = doc.getInteger("mid");
            int uid = doc.getInteger("uid");
            Double score = doc.getDouble("score");
            int timestamp = doc.getInteger("timestamp");

            Rating rating = new Rating(mid, uid, score, timestamp);
            // update current movie's rating list
            Movie movie = this.movieMap.get(mid);
            if (movie != null) {
                movie.addRating(rating);
            }
            if (this.userMap.containsKey(uid)) {
                User user = new User();
                user.setUserId(uid);
                this.userMap.put(uid, user);
            }
            this.userMap.get(uid).addRating(rating);
            count++;
        }
        System.out.println("Loading " + count + " ratings data completed. ");
        count = 0;

        // load link data
        MongoCollection<Document> links = db.getCollection(linkTable);
        for (Document doc : links.find()) {
            int mid = doc.getInteger("movieId");
            String imdbId = doc.getString("imdbId");
            String tmdbId = doc.getString("tmdbId");

            Movie movie = this.movieMap.get(mid);
            if (movie != null) {
                count ++;
                movie.setImdbId(imdbId);
                movie.setTmdbId(tmdbId);
            }
        }
        System.out.println("Loading " + count + " links data completed. ");
    }

    public void LoadStatisticsRecsData(String dbName, String rateMostMovies, String rateMostRecentlyMovies, String highScoreMovies, String genresTopMovies) {
        MongoDatabase db = MongoDBClient.getInstance().getDatabase(dbName);


    }

    public void LoadLFMRecsData(String lfmUserMovieRecs, String lfmRelatedMovies, String lfmSimUsers) {

    }
}
