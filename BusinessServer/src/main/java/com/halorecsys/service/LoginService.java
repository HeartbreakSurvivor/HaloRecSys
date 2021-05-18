package com.halorecsys.service;

import com.halorecsys.modules.UserModule;

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

            response.setContentType("pplication/json");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Access-Control-Allow-Origin", "*");

            String username = request.getParameter("username");
            String password = request.getParameter("password");
            System.out.println("username: "+ username + " password: " + password);

            StringBuilder msg = new StringBuilder("");
            PrintWriter out = response.getWriter();
            if (UserModule.CheckUserInfo(username, password)) {
                System.out.println("success");

                HttpSession session = request.getSession(true);
                if (null != session) {
                    session.setAttribute("username", username);
                }
                msg.append("{\"res\":\"success\"}");
            } else {
                System.out.println("failed");
                msg.append("{\"res\":\"failed\", \"msg\":\"用户名或者密码错误。\"}");
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
