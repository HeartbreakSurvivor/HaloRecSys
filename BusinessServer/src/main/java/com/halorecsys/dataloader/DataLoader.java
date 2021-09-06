package com.halorecsys.dataloader;

import com.google.common.collect.HashBiMap;
import com.halorecsys.utils.Config;
import com.halorecsys.utils.MongoDBClient;
import com.mongodb.BasicDBList;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.sun.tools.javac.util.Pair;
import org.bson.Document;

import java.util.*;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;

import com.google.common.collect.BiMap;


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
    HashMap<String, User> userMap; // username和User之间的映射关系

    // userIdx和username之间的映射关系
    HashMap<Integer, String> userId2Name;
    // movieIx和movieIdx之间的映射关系,方便双向查找
    BiMap<Integer, Integer> movieEmbBiMap;

    // map all movies to specific categories
    HashMap<String, List<Integer>> genresMap;

    // statistic recommendation
    // Number of movie recommendations
    int topN = 10;
    // Rated most movies
    List<Integer> rateMostMovies;
    // highest average score movies
    List<Integer> highScoreMovies;
    // Rated most recently movies
    List<Integer> rateRecentlyMovies;
    // top N high average score in categories
    HashMap<String, List<Integer>> genresTopMovies;

    // Collaborative Filter Recommendation
    List<Integer> lfmRecMovies;

    // TF-IDF based Recommendation
    List<Integer> TFIDFRecMovies;

    private DataLoader() {
        this.movieMap = new HashMap<>();
        this.userMap = new HashMap<>();
        this.userId2Name = new HashMap<>();
        this.movieEmbBiMap = HashBiMap.create();
        this.genresMap = new HashMap<>();

        this.rateMostMovies = new ArrayList<>();
        this.highScoreMovies = new ArrayList<>();
        this.rateRecentlyMovies = new ArrayList<>();
        this.genresTopMovies = new HashMap<>();

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
        } else {
            // 提取 () 内的发行年份
            String yearString = rawTitle.trim().substring(rawTitle.length()-5, rawTitle.length()-1);
            try{
                return Integer.parseInt(yearString);
            }catch (NumberFormatException exception){
                return -1;
            }
        }
    }

    public void LoadMovieData(String dbName, String movieTable, String ratingTable, String linkTable, String userTable, String newRatingTable) {
        MongoDatabase db = MongoDBClient.getInstance().getDatabase(dbName);

        int count = 0;
        // load movie data
        MongoCollection<Document> movies = db.getCollection(movieTable);
        for (Document doc : movies.find()) {
            // parse each movie
            int mid = doc.getInteger("mid");
            String name = doc.getString("name");
            String genres = doc.getString("genres");
            int midx = doc.getInteger("movieIdx");

            int releaseYear = parseReleaseYear(name);

            Movie m = new Movie();
            m.setMovieId(mid);
            m.setMovieIdx(midx);
            this.movieEmbBiMap.put(mid, midx);
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
                        genresMap.get(g).add(mid);
                    } else {
                        genresMap.put(g, new ArrayList<>());
                    }
                }
            }
            count++;
            this.movieMap.put(mid, m);
        }
        System.out.println("Total movie genres: ");
        System.out.println(this.genresMap.keySet());
        System.out.println("Loading " + count + " movies completed. ");
        count = 0;

        //load user data
        MongoCollection<Document> users = db.getCollection(userTable);
        for (Document doc : users.find()) {
            // parse each ratings data
            String username = doc.getString("username");
            int userId = doc.getInteger("userIdx");

            if (null != username && !this.userId2Name.containsKey(userId)) {
                this.userId2Name.put(userId, username);
            }
            if (null != username && !this.userMap.containsKey(username)) {
                User user = new User();
                user.setUserName(username);
                user.setUserId(userId);
                // 记录每个用户的兴趣爱好
                List<String> prefGenres = (List<String>)doc.get("preGenres");
                if (null != prefGenres) {
                    List<String> genres = new ArrayList<String>();
                    for (Object g : prefGenres) {
                        genres.add((String)g);
                    }
                    user.setPrefGenres(genres);
                }
                this.userMap.put(username, user);
            }
        }

        // load rating data
        MongoCollection<Document> ratings = db.getCollection(ratingTable);
        for (Document doc : ratings.find()) {
            // parse each ratings data
            int mid = doc.getInteger("mid");
            String username = doc.getString("uid");
            Double score = doc.getDouble("score");
            int timestamp = doc.getInteger("timestamp");

            Rating rating = new Rating(username, mid, score, timestamp);
            // update current movie's rating list
            Movie movie = this.movieMap.get(mid);
            if (movie != null) {
                movie.addRating(rating);
            }
            this.userMap.get(username).addRating(rating);
            if (score > 3.0) { //将评分大于3分的电影的类别设置为用户的喜爱类别
                List<String> genres = movie.getGenres();
                this.userMap.get(username).setPrefGenres(genres);
            }
            count++;
        }
        System.out.println("Loading " + count + " ratings data completed. ");
        count = 0;

        // load new ratings data
        // actually if we use new ratings table, the rating table is unnecessary
//        MongoCollection<Document> newRatings = db.getCollection(newRatingTable);
//        for (Document doc : newRatings.find()) {
//            // parse each ratings data
//            int mid = doc.getInteger("mid"); // movie id
//            int midx = doc.getInteger("movieIdx"); // movie index
//
//            String username = doc.getString("uid"); // user id
//            long userIdx = doc.getInteger("userIdx"); // user index
//
//            Double score = doc.getDouble("score");
//            int timestamp = doc.getInteger("timestamp");
//
//            Rating rating = new Rating(username, mid, score, timestamp);
//            // update current movie's rating list
//            Movie movie = this.movieMap.get(mid);
//            if (movie != null) {
//                movie.addRating(rating);
//            }
//
////            if (!this.userMap.containsKey(username)) {
////                User user = new User();
////                user.setUserName(username);
////                this.userMap.put(username, user);
////            }
//            this.userMap.get(username).addRating(rating);
//            if (score > 3.0) { //将评分大于3分的电影的类别设置为用户的喜爱类别
//                List<String> genres = movie.getGenres();
//                this.userMap.get(username).setPrefGenres(genres);
//            }
//            count++;
//        }
//        System.out.println("Loading " + count + " new ratings data completed. ");
//        count = 0;

        // load link data
        MongoCollection<Document> links = db.getCollection(linkTable);
        for (Document doc : links.find()) {
            int mid = doc.getInteger("mid");
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
        // get rate most movies
        MongoCollection<Document> rateMoreMoviesCollection = MongoDBClient.getInstance().getDatabase(dbName).getCollection(rateMostMovies);
        FindIterable<Document> rateMoreMovies = rateMoreMoviesCollection.find().sort(Sorts.descending("count")).limit(this.topN);
        for (Document doc : rateMoreMovies) {
            this.rateMostMovies.add(doc.getInteger("mid"));
        }

        // get rate most recently movies
        FindIterable<Document> rateMoreRecentlyMovies = MongoDBClient.getInstance().getDatabase(dbName).getCollection(rateMostRecentlyMovies).find()
                .sort(Sorts.descending("yearmonth")).limit(this.topN);
        for (Document doc : rateMoreRecentlyMovies) {
            this.rateRecentlyMovies.add(doc.getInteger("mid"));
        }

        // get top N high score movies
        FindIterable<Document> averageMovies = MongoDBClient.getInstance().getDatabase(dbName).getCollection(highScoreMovies).find()
                .sort(Sorts.descending("avg")).limit(this.topN);
        for (Document doc : averageMovies) {
            this.highScoreMovies.add(doc.getInteger("mid"));
        }

        // get top N score movies for each genre
        FindIterable<Document> genresTopMovieList = MongoDBClient.getInstance().getDatabase(dbName).getCollection(genresTopMovies).find();
        for (Document doc : genresTopMovieList) {
            String genre = doc.getString("genres");
            List<Pair<Integer,Double>> movielist = new ArrayList<>();
            ArrayList<Document> recs = doc.get("recs", ArrayList.class);
            for (Document recDoc : recs) {
                movielist.add(new Pair<Integer,Double>(recDoc.getInteger("mid"), recDoc.getDouble("score")));
            }
            // 对每个分组列表进行排序
            Collections.sort(movielist, new Comparator< Pair<Integer, Double> >() {
                @Override
                public int compare(final Pair<Integer, Double> o1, final Pair<Integer, Double> o2) {
                    return o1.snd > o2.snd ? -1: 1;// 降序排列
            }
            });
            List<Integer> temp = movielist.stream().map(p -> p.fst).collect(Collectors.toList());
            this.genresTopMovies.put(genre, temp);
        }
    }

    public List<Movie> getMoviesByType(int type, String genre, int size, String sortBy) {
        List<Movie> movies = new ArrayList<>();
        switch (type) {
            case 0: //根据类别推荐
                if (null != genre) {
                    for (int mid : this.genresMap.get(genre)) {
                        movies.add(this.movieMap.get(mid));
                    }
                    switch (sortBy) {
                        case "rating": movies.sort((m1, m2) -> Double.compare(m2.getAverageRating(), m1.getAverageRating()));break;
                        case "releaseYear": movies.sort((m1, m2) -> Integer.compare(m2.getReleaseYear(), m1.getReleaseYear()));break;
                        default: break;
                    }
                }
                break;
            case 1: //离线统计推荐
                switch (genre) {
                    case "Most Comments": // 最多评分电影
                        for (int mid : this.rateMostMovies)
                            movies.add(this.movieMap.get(mid));
                        break;
                    case "Highest Score": // 评分最高
                        for (int mid : this.highScoreMovies)
                            movies.add(this.movieMap.get(mid));
                        break;
                    case "Genres TopN": // 每个类别评分最高
                        for (String g : this.genresTopMovies.keySet()) {
                            // 去重
                            for (int mIdx : this.genresTopMovies.get(g)) {
                                if (!movies.contains(this.movieMap.get(mIdx))) {
                                    movies.add(this.movieMap.get(mIdx));
                                    break;
                                }
                            }
                        }
                        break;
                    case "Comments Recently": // 最近评论
                        for (int mid : this.rateRecentlyMovies)
                            movies.add(this.movieMap.get(mid));
                        break;
                    default: return null;
                }
                break;
            default:
                return null;
        }

        if (movies.size() > size) {
            return movies.subList(0, size);
        }
        return movies;
    }

    public List<Movie> getStreamingRecList(String username, String sortBy, int size) {
        List<Movie> res = new ArrayList();

        MongoCollection<Document> streamingCollection = MongoDBClient.getInstance().getDatabase(Config.DATABASE_NAME).getCollection(Config.MONGODB_STREAM_RECS_COLLECTION);
        Document docs = streamingCollection.find(eq("uid", username)).first();
        if (null != docs) {
            List<Integer> movielist = new ArrayList<>();
            ArrayList<Document> recs = docs.get("recs", ArrayList.class);
            int count = 0;
            for (Document doc : recs) {
                movielist.add(doc.getInteger("mid"));
                count ++;
                if (count == size) {
                    break;
                }
            }
            for (Integer t : movielist) {
                res.add(this.movieMap.get(t));
            }
        } else { // if user has logged in, but there is no streaming recs for him/her, then return the most rated movies list.
            for (int mid : this.rateMostMovies)
                res.add(this.movieMap.get(mid));
        }
        // 去重
        User user = this.userMap.get(username);
        for (Rating rating : user.getRatings()) {
            Movie m = this.movieMap.get(rating.getMovieId());
            if (res.contains(m)) {
                res.remove(m);
            }
        }
        switch (sortBy) {
            case "rating": res.sort((m1, m2) -> Double.compare(m2.getAverageRating(), m1.getAverageRating()));break;
            case "releaseYear": res.sort((m1, m2) -> Integer.compare(m2.getReleaseYear(), m1.getReleaseYear()));break;
            default: break;
        }
        return res;
    }

    private List<Movie> coldStartRecList(String username, int userId, int size) {
        // 对于初次注册的用户，根据其选择的喜爱的电影类别，取TopN，然后随机选出size个作为推荐结果
        List<Movie> res = new ArrayList();
        HashSet<Integer> tmp = new HashSet();
        for (String g : this.userMap.get(username).getPrefGenres()) {
            List<Integer> movies = this.genresTopMovies.get(g);
            if (movies != null) {
                for (int mIdx : movies) {
                    tmp.add(mIdx);
                }
            }
        }

        Iterator it = tmp.iterator();
        int count = 0;
        while(it.hasNext() && count < size){
            int mIdx = (Integer) it.next();
            res.add(this.movieMap.get(mIdx));
            count++;
        }
        return res;
    }

    public List<Movie> getUserRecList(String userName, int size, String mode) {
        if (null == mode) return null;
        int userId = DataLoader.getInstance().getUserByName(userName).getUserId();
        switch (mode) {
            case "lfm": // 通过LFM协同过滤算法来计算用户电影推荐列表
                MongoCollection<Document> similarMovieTable = MongoDBClient.getInstance().getDatabase(Config.DATABASE_NAME).getCollection(Config.LFM_USER_RECS);
                Document docs = similarMovieTable.find(eq("uid", userId)).first();
                if (null != docs) {
                    List<Integer> movielist = new ArrayList<>();
                    ArrayList<Document> recs = docs.get("recs", ArrayList.class);
                    int count = 0;
                    for (Document doc : recs) {
                        movielist.add(doc.getInteger("mid"));
                        count ++;
                        if (count == size) {
                            break;
                        }
                    }
                    List<Movie> res = new ArrayList<>();
                    for (Integer t : movielist) {
                        res.add(this.movieMap.get(t));
                    }
                    return res;
                } else {
                    return coldStartRecList(userName, userId, size);
                }
            case "emb": // 通过embedding向量来计算用户电影推荐列表
                break;
            default:
                break;
        }
        return null;
    }

    public List<User> getSimilarUsers(String userName, int size, String mode) {
        if (null == mode) return null;
        switch (mode) {
            case "lfm": //根据协同过滤算法来计算电影相似度
                MongoCollection<Document> similarMovieTable = MongoDBClient.getInstance().getDatabase(Config.DATABASE_NAME).getCollection(Config.LFM_USER_SIM_RECS);
                int userId = DataLoader.getInstance().getUserByName(userName).getUserId();
                Document docs = similarMovieTable.find(eq("uid", userId)).first();
                if (null != docs) {
                    // find the most topN similar movies
                    List<User> userList = new ArrayList<>();
                    ArrayList<Document> recs = docs.get("sims", ArrayList.class);
                    int count = 0;
                    for (Document recDoc : recs) {
                        int uid = recDoc.getInteger("uid");
                        userList.add(this.userMap.get(this.userId2Name.get(uid)));
                        count ++;
                        if (count == size) {
                            break;
                        }
                    }
                    return userList;
                }
                break;
            case "emb":
                break;
            default:
                return null;
        }
        return null;
    }

    public List<Movie> getSimilarMovies(int movieId, int size, String mode) {
        if (null == mode) return null;
        switch (mode) {
            case "lfm": //根据协同过滤算法来计算电影相似度
                MongoCollection<Document> similarMovieTable = MongoDBClient.getInstance().getDatabase(Config.DATABASE_NAME).getCollection(Config.LFM_MOVIE_RECS);
                Document docs = similarMovieTable.find(eq("mid", movieId)).first();
                if (null != docs) {
                    // find the most topN similar movies
                    List<Pair<Integer,Double>> movielist = new ArrayList<>();
                    ArrayList<Document> recs = docs.get("recs", ArrayList.class);
                    int count = 0;
                    for (Document recDoc : recs) {
                        movielist.add(new Pair<Integer,Double>(recDoc.getInteger("mid"), recDoc.getDouble("score")));
                        count ++;
                        if (count == size) {
                            break;
                        }
                    }
//                    for (Pair<Integer, Double> m : movielist) {
//                        System.out.println(m.snd + " ");
//                    }
                    List<Integer> temp = movielist.stream().map(p -> p.fst).collect(Collectors.toList());
                    List<Movie> res = new ArrayList<>();
                    for (Integer t : temp) {
                        res.add(this.movieMap.get(t));
                    }
                    return res;
                }
                break;
            case "emb":
                break;
            default:
                return null;
        }
        return null;
    }

    //get movie object by movie id
    public Movie getMovieById(int movieId){
        return this.movieMap.get(movieId);
    }

    public Set<Integer> getTotalMovieIds() {
        // a weird Bug 2021.6.4
        // any operation on the original keySet() will modify the movieMap
        // so we need to return a copy of movieMap.keySet()

        //return this.movieMap.keySet();

        // this can fix the bug, but it's not efficient, cause we need to construct a
        // same new Set every time.
        Set<Integer> copy = new HashSet<>();
        Iterator<Integer> it = this.movieMap.keySet().iterator();
        while (it.hasNext()) {
            Integer t = it.next();
            copy.add(t);
        }
        return copy;
    }

    public Integer getMovieIdOrMovieIdx(int key, boolean inverse) {
        if (inverse) {
            return this.movieEmbBiMap.inverse().get(key);
        }
        return this.movieEmbBiMap.get(key);
    }

    //get user object by user id
    public User getUserByName(String userName){ return this.userMap.get(userName); }

    //add a new user object by user id
    public void setUser(String username, int userId){
        User user = new User();
        user.setUserName(username);
        user.setUserId(userId);
        System.out.println(username);
        this.userMap.put(username, user);
    }
}
