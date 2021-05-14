package com.halorecsys.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.halorecsys.dataloader.DataLoader;
import com.halorecsys.dataloader.User;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @program: HaloRecSys
 * @description: return information of a specific user
 * @author: HaloZhang
 * @create: 2021-05-14 15:31
 **/
public class UserService extends HttpServlet {
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response) throws IOException {
        try {
            System.out.println(UserService.class.getName() + "doGet...............");

            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Access-Control-Allow-Origin", "*");

            //get user id via url parameter
            String userId = request.getParameter("id");

            //get user object from DataManager
            User user = DataLoader.getInstance().getUserById(Integer.parseInt(userId));

            //convert movie object to json format and return
            if (null != user) {
                ObjectMapper mapper = new ObjectMapper();
                String jsonUser = mapper.writeValueAsString(user);
                response.getWriter().println(jsonUser);
            }else{
                response.getWriter().println("");
            }

        } catch (Exception e) {
            e.printStackTrace();
            response.getWriter().println("");
        }
    }
}
