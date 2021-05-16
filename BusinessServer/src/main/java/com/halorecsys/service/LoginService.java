package com.halorecsys.service;

import com.halorecsys.modules.UserModule;
import com.halorecsys.utils.Config;
import com.halorecsys.utils.MongoDBClient;
import com.mongodb.client.MongoDatabase;

import javax.servlet.RequestDispatcher;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * @program: HaloRecSys
 * @description: user login service
 * @author: HaloZhang
 * @create: 2021-05-16 15:23
 **/
public class LoginService extends HttpServlet {
    protected void doPost(HttpServletRequest request,
                          HttpServletResponse response) throws IOException {
        try {
            System.out.println(LoginService.class.getName() + "...doPost...");

            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Access-Control-Allow-Origin", "*");

            String username = request.getParameter("username");
            String password = request.getParameter("password");
            System.out.println("username: "+ username + " password: " + password);

            PrintWriter out = response.getWriter();
            if (UserModule.CheckUserInfo(username, password)) {
                HttpSession session = request.getSession();
                session.setAttribute("username", username);
                RequestDispatcher rd = request.getRequestDispatcher("welcome");
                rd.forward(request, response);
            } else {
                out.print("用户或密码错误");
                RequestDispatcher rd = request.getRequestDispatcher("index.html");
                rd.include(request, response);
            }
            out.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            response.getWriter().println("");
        }
    }
}
