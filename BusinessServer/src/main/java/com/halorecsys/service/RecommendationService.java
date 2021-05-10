package com.halorecsys.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.halorecsys.dataloader.DataLoader;
import com.halorecsys.dataloader.Movie;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Enumeration;
import java.util.List;

/**
 * @program: HaloRecSys
 * @description: recommend different movies to front
 * @author: HaloZhang
 * @create: 2021-05-09 15:46
 **/
public class RecommendationService extends HttpServlet {
//    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws
//            ServletException, IOException {
//        try {
//            System.out.println(RecommendationService.class.getName() + " doGet() " );
//            System.out.println("request url: " + request.getRequestURI());
//
//            response.setContentType("application/json");
//            response.setStatus(HttpServletResponse.SC_OK);
//            response.setCharacterEncoding("UTF-8");
//            response.setHeader("Access-Control-Allow-Origin", "*");
//
//            //get parameters from query
//            String type = request.getParameter("type");
//            String genre = request.getParameter("genre");
//            String size = request.getParameter("size");
//            String sortby = request.getParameter("sortby");
//
//            System.out.println("type: " + type + " genre: " + genre + " size: " + size + " sortby: " + sortby);
//            List<Movie> movies = DataLoader.getInstance().getMoviesByType(Integer.parseInt(type), genre, Integer.parseInt(size), sortby);
//
//            //convert movie list to json format and return
//            ObjectMapper mapper = new ObjectMapper();
//            String jsonMovies = mapper.writeValueAsString(movies);
//            response.getWriter().println(jsonMovies);
//        } catch (Exception e) {
//            e.printStackTrace();
//            response.getWriter().println(""); // 返回空内容
//        }
//    }

    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response) throws ServletException,
            IOException {
        try {
            System.out.println(RecommendationService.class.getName() + " doGet...............");
            System.out.println("request url: " + request.getRequestURI());

            System.out.println("request parameters: ");
            Enumeration e = request.getParameterNames();
            while (e.hasMoreElements()) {
                String name = (String)e.nextElement();
                String value = request.getParameter(name);
                System.out.println(name + " = " + value);
            }

            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Access-Control-Allow-Origin", "*");

            //genre - movie category
            String genre = request.getParameter("genre");
            //number of returned movies
            String size = request.getParameter("size");
            //ranking algorithm
            String sortby = request.getParameter("sortby");
            // get type
            String type = request.getParameter("type");

            System.out.println("type: " + type + " genre: " + genre + " size: " + size + " sortby: " + sortby);

            //a simple method, just fetch all the movie in the genre
            List<Movie> movies = DataLoader.getInstance().getMoviesByGenre(genre, Integer.parseInt(size),sortby);

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
