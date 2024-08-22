/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package com.synclite.dbreader.web;


import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.servlet.AsyncContext;
import javax.servlet.DispatcherType;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class ValidateDestinationDB
 */
@WebServlet("/validateNumSchedules")
public class ValidateNumSchedules extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor. 
	 */
	public ValidateNumSchedules() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}


	/**	  
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		try {
	
			String numScedulesStr = request.getParameter("synclite-dbreader-scheduler-num-schedules");
			try {
				if (Integer.valueOf(numScedulesStr) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Number of Schedules\"");
				} else if (Integer.valueOf(numScedulesStr) <= 0 ) {
					throw new ServletException("Please specify a positive numeric value for \"Number of Schedules\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Number of Schedules\"");
			}

			request.getSession().setAttribute("synclite-dbreader-scheduler-num-schedules", numScedulesStr);
			
			//request.getRequestDispatcher("configureScheduler.jsp").forward(request, response);
			response.sendRedirect("configureScheduler.jsp");

		} catch (Exception e) {
			System.out.println("exception : " + e);
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("configureNumSchedules.jsp?errorMsg=" + errorMsg).forward(request, response);
		}
	}
}
