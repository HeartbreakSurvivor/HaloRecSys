package com.halorecsys.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.halorecsys.dataloader.DataLoader;
import com.halorecsys.dataloader.Movie;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: HaloRecSys
 * @description: get related movies
 * @author: HaloZhang
 * @create: 2021-05-13 16:28
 **/
public class SimilarMovieService extends HttpServlet {
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response) throws IOException {
        try {
            System.out.println(SimilarMovieService.class.getName() + "doGet...............");

            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Access-Control-Allow-Origin", "*");

            //movieId
            String movieId = request.getParameter("movieId");
            //number of returned movies
            String size = request.getParameter("size");
            //mode of calculating similarity, e.g. lfm, embedding, graph-embedding
            String mode = request.getParameter("mode");

            //use SimilarMovieFlow to get similar movies
            List<Movie> movies = DataLoader.getInstance().getSimilarMovies(Integer.parseInt(movieId), Integer.parseInt(size), mode);

            //convert movie list to json format and return
            ObjectMapper mapper = new ObjectMapper();
            String jsonMovies = mapper.writeValueAsString(movies);
            response.getWriter().println(jsonMovies);
        } catch (Exception e) {
            e.printStackTrace();
            response.getWriter().println("");
        }
    }
}
