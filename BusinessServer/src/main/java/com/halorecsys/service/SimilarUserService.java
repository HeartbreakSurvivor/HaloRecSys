package com.halorecsys.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.halorecsys.dataloader.DataLoader;
import com.halorecsys.dataloader.Movie;
import com.halorecsys.dataloader.User;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

/**
 * @program: HaloRecSys
 * @description: get the similar users for a specific user
 * @author: HaloZhang
 * @create: 2021-05-14 15:48
 **/
public class SimilarUserService extends HttpServlet {
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response) throws IOException {
        try {
            System.out.println(SimilarUserService.class.getName() + "doGet...............");

            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Access-Control-Allow-Origin", "*");

            //movieId
            String userId = request.getParameter("userId");
            //number of returned movies
            String size = request.getParameter("size");
            //mode of calculating similarity, e.g. lfm, embedding, graph-embedding
            String mode = request.getParameter("mode");

            //use different model to get similar users
            List<User> users = DataLoader.getInstance().getSimilarUsers(Integer.parseInt(userId.substring(4)), Integer.parseInt(size), mode);

            //convert movie list to json format and return
            ObjectMapper mapper = new ObjectMapper();
            String jsonMovies = mapper.writeValueAsString(users);
            response.getWriter().println(jsonMovies);
        } catch (Exception e) {
            e.printStackTrace();
            response.getWriter().println("");
        }
    }
}
