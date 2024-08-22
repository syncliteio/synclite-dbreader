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
import java.io.IOException;
import java.io.InputStreamReader;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class StopJob
 */
@WebServlet("/stopJob")
public class StopJob extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public StopJob() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		//Get current job PID if running
		try {
			if (request.getSession().getAttribute("synclite-device-dir") == null) {
				response.sendRedirect("syncLiteTerms.jsp");			
			} else {
				String syncLiteDeviceDir = request.getSession().getAttribute("synclite-device-dir").toString();
				long currentJobPID = 0;
				Process jpsProc;
				if (isWindows()) {
					String javaHome = System.getenv("JAVA_HOME");			
					String scriptPath = "jps";
					if (javaHome != null) {
						scriptPath = javaHome + "\\bin\\jps";
					} else {
						scriptPath = "jps";
					}
					String[] cmdArray = {scriptPath, "-l", "-m"};
					jpsProc = Runtime.getRuntime().exec(cmdArray);
				} else {
					String javaHome = System.getenv("JAVA_HOME");			
					String scriptPath = "jps";
					if (javaHome != null) {
						scriptPath = javaHome + "/bin/jps";
					} else {
						scriptPath = "jps";
					}
					String[] cmdArray = {scriptPath, "-l", "-m"};
					jpsProc = Runtime.getRuntime().exec(cmdArray);
				}
		
				BufferedReader stdout = new BufferedReader(new InputStreamReader(jpsProc.getInputStream()));
				String line = stdout.readLine();
				while (line != null) {
					if (line.contains("com.synclite.dbreader.Main") && line.contains(syncLiteDeviceDir)) {
						currentJobPID = Long.valueOf(line.split(" ")[0]);
					}
					line = stdout.readLine();
				}
				//stdout.close();
		
				//Kill job if found
		
				if(currentJobPID > 0) {
					if (isWindows()) {
						Runtime.getRuntime().exec("taskkill /F /PID " + currentJobPID);
					} else {
						Runtime.getRuntime().exec("kill -9 " + currentJobPID);
					}
				}
				request.getSession().setAttribute("job-status","STOPPED");
				//request.getRequestDispatcher("dashboard.jsp").forward(request, response);
				response.sendRedirect("dashboard.jsp");			
			}
		} catch(Exception e) {
			String errorMsg = e.getMessage();
			//request.getRequestDispatcher("jobError.jsp?jobType=Stop&errorMsg=" + errorMsg).forward(request, response);
			response.sendRedirect("jobError.jsp?jobType=StartRead&errorMsg=" + errorMsg);
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
	
	private boolean isWindows() {
		return System.getProperty("os.name").startsWith("Windows");
	}

}
