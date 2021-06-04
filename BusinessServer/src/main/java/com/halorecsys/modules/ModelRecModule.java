package com.halorecsys.modules;

import com.halorecsys.dataloader.DataLoader;
import com.halorecsys.dataloader.Movie;
import com.halorecsys.dataloader.Rating;
import com.halorecsys.dataloader.User;
import com.halorecsys.utils.Config;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

import com.halorecsys.utils.HttpClient;

/**
 * @program: HaloRecSys
 * @description: movie recommendation use deep learning model
 * @author: HaloZhang
 * @create: 2021-06-03 14:39
 **/
public class ModelRecModule {
    static public List<Movie> getModelRecList(String username, String model, int size) {
        // find the user movie recommendation candidates
        List<Integer> movieCandidates = getUserMovieCandidates(username);
        // construct http url
        String url = Config.TORCH_SERVE_URL + model;
        System.out.println("Torch server Host " + url);
        switch (model) {
            case "ncf":
                return NCFRecModel(username, movieCandidates, size, url);
            default: break;
        }

        return null;
    }

    static private List<Integer> getUserMovieCandidates(String username) {
        User user = DataLoader.getInstance().getUserByName(username);
        // get user watched movies
        List<Rating> userRatings = user.getRatings();
        Set<Integer> userWatchedMovies = new HashSet<>();
        for (Rating ur : userRatings) {
            userWatchedMovies.add(ur.getMovieId());
        }

        // get user recommendation movies candidates
        Set<Integer> totalMovieIds = DataLoader.getInstance().getTotalMovieIds();
        System.out.println("length of totalMovies: " + totalMovieIds.size());
        System.out.println("length of user watched Movies: " + userWatchedMovies.size());

        // obtain different set
        Set<Integer> userUnWatchedMovieIds = totalMovieIds;
        userUnWatchedMovieIds.removeAll(userWatchedMovies);

        System.out.println("length of user unwatched Movies: " + userUnWatchedMovieIds.size());

        return new ArrayList<>(userUnWatchedMovieIds);
    }

    static private List<Movie> NCFRecModel(String username, List<Integer> candidates, int size, String url) {
        // 1. get user embedding index
        Integer userEmbIdx = DataLoader.getInstance().getUserByName(username).getUserId();

        // 2. get movie embedding index
        List<Integer> movieEmbIdx = new ArrayList<>();
        for (int mid : candidates) {
            // get movie Index by movieId
            movieEmbIdx.add(DataLoader.getInstance().getMovieById(mid).getMovieIdx());
        }

        // 3. construct http request json
        JSONArray instances = new JSONArray();
        //System.out.println("movieEmbIdx: " + movieEmbIdx);

        for (int mIdx : movieEmbIdx) {
            JSONObject tmp = new JSONObject();
            tmp.put("user_idx", userEmbIdx);
            tmp.put("item_idx", mIdx);
            instances.put(tmp);
        }
        JSONObject testJson = new JSONObject();
        testJson.put("test", instances);
        //System.out.println(testJson.toString());

        // 4. send the request and parse the response
        //need to confirm the torch serving end point
        String predictions = HttpClient.asyncSinglePostRequest(url, testJson.toString());
        System.out.println("send " + username + " request to torch serving.");
        //System.out.println("predictions: \n" +  predictions);

        // construct a json format string
        String jsonStr = "{\"res\":" +  predictions + "}";

        //System.out.println("jsonStr: \n" +  jsonStr);

        JSONObject predictionsObject = new JSONObject(jsonStr);
        HashMap<Integer, Double> candidateScoreMap = new HashMap<>();

        JSONArray scores = predictionsObject.getJSONArray("res");
        for (int i = 0 ; i < candidates.size(); i++){
            candidateScoreMap.put(candidates.get(i), scores.getDouble(i));
        }

        // 5. sort the predictions with score
        List<Map.Entry<Integer, Double>> list = new ArrayList<Map.Entry<Integer, Double>>(candidateScoreMap.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<Integer, Double>>() {
            public int compare(Map.Entry<Integer, Double> o1, Map.Entry<Integer, Double> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        // 6. select the specific numbers of movies
        List<Map.Entry<Integer, Double>> newList = list.subList(0, size);

        // 7. convert movie embedding index to movieId
        List<Movie> res = new ArrayList<>();
        for (Map.Entry<Integer, Double> m : newList) {
            int mIdx = m.getKey();
            // get movieId by movie Index
            int movieId = DataLoader.getInstance().getMovieIdOrMovieIdx(mIdx, true);
            Movie movie = DataLoader.getInstance().getMovieById(movieId);
            res.add(movie);
        }
        return res;
    }
}
