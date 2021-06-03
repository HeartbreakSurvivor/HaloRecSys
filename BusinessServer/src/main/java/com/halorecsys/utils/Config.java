package com.halorecsys.utils;

/**
 * @program: HaloRecSys
 * @description: some project configurations
 * @author: HaloZhang
 * @create: 2021-05-06 14:30
 **/
public class Config {
    // 程序默认端口配置
    public static final int DEFAULT_PORT = 3506;

    // MongoDB table name definition
    public static final String DATABASE_NAME = "recommender";
    public static final String MONGODB_MOVIE_COLLECTION = "Movies";
    public static final String MONGODB_RATING_COLLECTION = "Ratings";
    public static final String MONGODB_NEW_RATING_COLLECTION = "NewRatings";
    public static final String MONGODB_LINK_COLLECTION = "Links";
    public static final String MONGODB_TAG_COLLECTION = "Tags";
    public static final String MONGODB_USER_COLLECTION = "Users";

    public static final String ES_MOVIE_INDEX = "Movie";

    /********** 离线统计推荐表 ***********/
    //总体评分次数最多的电影
    public static final String RATE_MOST_MOVIES = "RateMostMovies";
    //最近评分次数最多的电影
    public static final String RATE_MOST_RECENTLY_MOVIES = "RateMostRecentlyMovies";
    //每部电影的平均评分
    public static final String AVERAGE_RATINGS_MOVIES = "AverageScoreMovies";
    //按类别划分评分最高的电影
    public static final String GENRES_TOP_N_MOVIES = "GenresTopMovies";

    /********** 协同过滤推荐表 ***********/
    // 每个用户的电影推荐列表
    public static final String LFM_USER_RECS = "LFM_USER_RECS";
    // 每部电影的相似度矩阵表
    public static final String LFM_MOVIE_RECS = "LFM_MOVIE_RECS";
    // 每个用户的相似度推荐
    public static final String LFM_USER_SIM_RECS = "LFM_USER_SIM_RECS";

    /*********** 实时推荐列表 ************/
    public static String MOVIE_RATING_PREFIX = "MOVIE_RATING_PREFIX";
    public static int REDIS_MOVIE_RATING_QUEUE_SIZE = 40;
    public static final String MONGODB_STREAM_RECS_COLLECTION = "StreamingRecs";

    /*********** 模型推荐相关 ************/
    public static final String TORCH_SERVE_URL = "http://127.0.0.1:8080/predictions/";

}
