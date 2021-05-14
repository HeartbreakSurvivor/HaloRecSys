package com.halorecsys.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.halorecsys.dataloader.DataLoader;
import com.halorecsys.dataloader.Movie;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @program: HaloRecSys
 * @description: get description of movie by movieId
 * @author: HaloZhang
 * @create: 2021-05-13 16:16
 **/
public class MovieService extends HttpServlet {
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response) throws IOException {
        try {
            System.out.println(MovieService.class.getName() + "doGet...............");
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Access-Control-Allow-Origin", "*");

            //get movie id via url parameter
            String movieId = request.getParameter("id");

            //get movie object from DataLoader
            Movie movie = DataLoader.getInstance().getMovieById(Integer.parseInt(movieId));

            //convert movie object to json format and return
            if (null != movie) {
                ObjectMapper mapper = new ObjectMapper();
                String jsonMovie = mapper.writeValueAsString(movie);
                response.getWriter().println(jsonMovie);
            } else {
                response.getWriter().println("");
            }

        } catch (Exception e) {
            e.printStackTrace();
            response.getWriter().println("");
        }
    }
}
