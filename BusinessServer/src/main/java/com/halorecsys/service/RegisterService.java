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
            String password = request.getParameter("password");
            System.out.println("username: "+ username + " password: " + password);

            PrintWriter out = response.getWriter();
            if (UserModule.RegisterUser(username, password)) {
                // 跳转到电影类别选择界面
                HttpSession session = request.getSession();
                session.setAttribute("username", username);
                RequestDispatcher rd = request.getRequestDispatcher("welcome");
                rd.forward(request, response);
            } else {
                out.print("用户名已存在!");
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
