package com.halorecsys.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.halorecsys.dataloader.DataLoader;
import com.halorecsys.dataloader.Movie;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

/**
 * @program: HaloRecSys
 * @description: recommend some movies for a specific user
 * @author: HaloZhang
 * @create: 2021-05-14 15:35
 **/
public class RecForYouService extends HttpServlet {
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response) throws ServletException,
            IOException {
        try {
            System.out.println(RecForYouService.class.getName() + "doGet...............");

            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Access-Control-Allow-Origin", "*");

            //get user id via url parameter
            String userName = request.getParameter("username");
            //number of returned movies
            String size = request.getParameter("size");
            //ranking algorithm
            String mode = request.getParameter("mode");

            int userId = DataLoader.getInstance().getUserByName(userName).getUserId();
            //a simple method, just fetch all the movie in the genre
            List<Movie> movies = DataLoader.getInstance().getUserRecList(userId, Integer.parseInt(size), mode);

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
