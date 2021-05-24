package com.halorecsys.service;

import javax.servlet.RequestDispatcher;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.io.PrintWriter;

import com.halorecsys.modules.UserModule;

/**
 * @program: HaloRecSys
 * @description: register a new user to system
 * @author: HaloZhang
 * @create: 2021-05-16 14:56
 **/
public class RegisterService extends HttpServlet {
    protected void doPost(HttpServletRequest request,
                         HttpServletResponse response) throws IOException {
        try {
            System.out.println(RegisterService.class.getName() + "...doPost...");

            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Access-Control-Allow-Origin", "*");

            String username = request.getParameter("username");
            System.out.println("username: " + username);

            String password = request.getParameter("password");
            String genres = request.getParameter("genre");

            PrintWriter out = response.getWriter();
            StringBuilder msg = new StringBuilder("");

            if (null != password) {
                if (UserModule.RegisterUser(username, password)) {
                    System.out.println("注册成功!");
                    msg.append("{\"res\":\"success\"}");
                } else {
                    System.out.println("用户名已存在!");
                    msg.append("{\"res\":\"failed\",\"msg\":\"用户名已存在!\"}");
                }
            }
            if (null != genres) {
                System.out.println("genres: " + genres);
                String[] genresBuf = genres.split(",");
                UserModule.SetUserPres(username, genresBuf);
                msg.append("{\"res\":\"success\"}");
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
