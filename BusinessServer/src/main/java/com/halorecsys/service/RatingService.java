package com.halorecsys.service;

import com.halorecsys.dataloader.DataLoader;
import com.halorecsys.dataloader.Rating;
import com.halorecsys.modules.RatingModule;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * @program: HaloRecSys
 * @description: collect user ratings
 * @author: HaloZhang
 * @create: 2021-05-16 15:25
 **/
public class RatingService extends HttpServlet {
    protected void doPost(HttpServletRequest request,
                          HttpServletResponse response) throws IOException {
        try {
            System.out.println(RatingService.class.getName() + "...doPost...");

            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Access-Control-Allow-Origin", "*");

            String username = request.getParameter("username");
            String movieId = request.getParameter("movieId");
            String score = request.getParameter("rating");
            String timestamp = request.getParameter("timestamp");

            PrintWriter out = response.getWriter();
            StringBuilder msg = new StringBuilder("");

            Rating rating = new Rating(username, Integer.parseInt(movieId), Double.parseDouble(score), Integer.parseInt(timestamp));

            // update user ratings
            DataLoader.getInstance().getUserByName(username).addRating(rating);

            // update redis and mongoDB
            if (RatingModule.movieRating(rating)) {
                msg.append("{\"res\":\"success\",\"msg\":\"打分成功!\"}");
            } else {
                msg.append("{\"res\":\"failed\",\"msg\":\"评分数据更新失败!\"}");
            }

            out.println(new String(msg));
            out.flush();
            out.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            response.getWriter().println("");
        }
    }
}
