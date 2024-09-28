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
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.json.JSONArray;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Types;

/**
 * Servlet implementation class ValidateJobConfiguration
 */
@WebServlet("/validateDeviceDirectory")
public class ValidateDeviceDirectory extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	private Logger globalTracer;

	/**
	 * Default constructor. 
	 */
	public ValidateDeviceDirectory() {
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doPost(request, response);
	}

	/**	  
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try {
			String jobName = request.getParameter("job-name");

			if (jobName != null) {
				//Check if specified jobName is in correct format
				if (jobName.length() > 16 ) {
					throw new ServletException("Job name must be upto 16 characters in length");
				}
				if (!jobName.matches("[a-zA-Z0-9-_]+")) {
					throw new ServletException("Specified job name is invalid. Allowed characters are alphanumeric characters, hyphens or underscores.");
				}		
			} else {
				jobName = "job1";
			}

			String syncLiteDeviceDirStr = request.getParameter("synclite-device-dir").toString();
			Path syncLiteDeviceDir;
			if ((syncLiteDeviceDirStr== null) || syncLiteDeviceDirStr.trim().isEmpty()) {
				throw new ServletException("\"SyncLite Device Directory Path\" must be specified");
			} else {
				syncLiteDeviceDir = Path.of(syncLiteDeviceDirStr);
				if (! Files.exists(syncLiteDeviceDir)) {
					try {
						Files.createDirectories(syncLiteDeviceDir);
					} catch (Exception e) {
						throw new ServletException("Failed to create device directory : " + syncLiteDeviceDirStr + " : " + e.getMessage(), e);
					}
				}
				if (! Files.exists(syncLiteDeviceDir)) {
					throw new ServletException("Specified \"SyncLite Device Directory Path\" : " + syncLiteDeviceDir + " does not exist, please specify a valid path.");
				}
			}
			
			if (! syncLiteDeviceDir.toFile().canRead()) {
				throw new ServletException("Specified \"SyncLite Device Directory Path\" does not have read permission");
			}

			if (! syncLiteDeviceDir.toFile().canWrite()) {
				throw new ServletException("Specified \"SyncLite Device Directory Path\" does not have write permission");
			}

			initTracer(syncLiteDeviceDir);

			request.getSession().setAttribute("job-name", jobName);
			request.getSession().setAttribute("synclite-device-dir", syncLiteDeviceDirStr);

			response.sendRedirect("configureDBReader.jsp");
		} catch (Exception e) {
			//System.out.println("exception : " + e);
			this.globalTracer.error("Exception while processing request:", e);
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("selectDeviceDirectory.jsp?errorMsg=" + errorMsg).forward(request, response);
			throw new ServletException(e);
		}
	}

	private final void initTracer(Path workDir) {
		this.globalTracer = Logger.getLogger(ValidateDeviceDirectory.class);    	
		globalTracer.setLevel(Level.INFO);
		if (this.globalTracer.getAppender("SyncLiteDBReaderTracer") == null) {
			RollingFileAppender fa = new RollingFileAppender();
			fa.setName("SyncLiteDBReaderTracer");
			fa.setFile(workDir.resolve("synclite_dbreader.trace").toString());
			fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
			fa.setMaxBackupIndex(10);
			fa.setAppend(true);
			fa.activateOptions();
			globalTracer.addAppender(fa);
		}
	}
}


