package com.halorecsys.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.halorecsys.dataloader.DataLoader;
import com.halorecsys.dataloader.Movie;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: HaloRecSys
 * @description: real time streaming movie recommendation
 * @author: HaloZhang
 * @create: 2021-05-28 14:16
 **/
public class StreamingRecService extends HttpServlet {
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws
            ServletException, IOException {
        try {
            System.out.println(StreamingRecService.class.getName() + " doGet() " );

            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Access-Control-Allow-Origin", "*");

            //get parameters from query
            String userName = request.getParameter("username");
            String size = request.getParameter("size");
            String sortby = request.getParameter("sortby");

            List<Movie> movies = DataLoader.getInstance().getStreamingRecList(userName, sortby, Integer.parseInt(size));

            //convert movie list to json format and return
            ObjectMapper mapper = new ObjectMapper();
            String jsonMovies = mapper.writeValueAsString(movies);
            response.getWriter().println(jsonMovies);
        } catch (Exception e) {
            e.printStackTrace();
            response.getWriter().println(""); // 返回空内容
        }
    }
}

