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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
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
@WebServlet("/manageObjects")
public class ManageObjects extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	private Logger globalTracer;

	/**
	 * Default constructor. 
	 */
	public ManageObjects() {
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	/**	  
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		try {
			Path syncLiteDeviceDir = Path.of(request.getSession().getAttribute("synclite-device-dir").toString());

			initTracer(syncLiteDeviceDir);

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
				if (line.contains("com.synclite.dbreader.Main") && line.contains(syncLiteDeviceDir.toString())) {
					currentJobPID = Long.valueOf(line.split(" ")[0]);
				}
				line = stdout.readLine();
			}
			if(currentJobPID != 0) {
				String errorMessage = "A job is running with Process ID : " + currentJobPID + ". Please stop the job and then use Manage Objects";
				request.getRequestDispatcher("manageObjects.jsp?errorMsg=" + errorMessage).forward(request, response);
			} else {

				Path dbReaderMetadataFilePath = Path.of(syncLiteDeviceDir.toString(), "synclite_dbreader_metadata.db");

				String numObjectsStr = request.getParameter("num-objects");

				Integer numObjects = 0;
				try {
					numObjects = Integer.valueOf(numObjectsStr);
				} catch (NumberFormatException e) {
					//
					//This was coming as the hidden variable was not defined in the form in case of a very large form with 2000+ 
					//objects listed on the configure tables page. We do not know why.
					//Hence, 
					//try querying the object info from the metadata file itself.
					//
					try {
						numObjects = getNumObjects(dbReaderMetadataFilePath);
					} catch (Exception e1) {
						throw new ServletException("Manage Tables/Views page was not loaded properly. Please try reloading again.", e);
					}
				}

				//Iterate on all tables and take actions accordingly.
				HashMap<String, Integer> enabled = new HashMap<String, Integer>();
				HashMap<String, Integer> reloadSchemaOnNextRestart= new HashMap<String, Integer>();
				HashMap<String, Integer> reloadSchemaOnEachRestart = new HashMap<String, Integer>();
				HashMap<String, Integer> reloadObjectOnNextRestart= new HashMap<String, Integer>();
				HashMap<String, Integer> reloadObjectOnEachRestart = new HashMap<String, Integer>();
				for (int i=0; i < numObjects; ++i) {
					String objectName = request.getParameter("object-name-" + i).strip();					
					if (request.getParameter("enable-" + i) != null) {
						enabled.put(objectName, 1);
					} else {
						enabled.put(objectName, 0);
					}

					if (request.getParameter("rsnr-" + i) != null) {
						reloadSchemaOnNextRestart.put(objectName, 1);
					} else {
						reloadSchemaOnNextRestart.put(objectName, 0);
					}

					if (request.getParameter("rser-" + i) != null) {
						reloadSchemaOnEachRestart.put(objectName, 1);
					} else {
						reloadSchemaOnEachRestart.put(objectName, 0);
					}

					if (request.getParameter("ronr-" + i) != null) {
						reloadObjectOnNextRestart.put(objectName, 1);
					} else {
						reloadObjectOnNextRestart.put(objectName, 0);
					}

					if (request.getParameter("roer-" + i) != null) {
						reloadObjectOnEachRestart.put(objectName, 1);
					} else {
						reloadObjectOnEachRestart.put(objectName, 0);
					}
				}

				try {
					updateObjectsInMetadataFile(dbReaderMetadataFilePath, enabled, reloadSchemaOnNextRestart, reloadSchemaOnEachRestart, reloadObjectOnNextRestart, reloadObjectOnEachRestart);
					int numEnabledObjects = getNumEnabledObjects(dbReaderMetadataFilePath);
					request.getSession().setAttribute("num-enabled-objects", numEnabledObjects);
				} catch (Exception e) {
					this.globalTracer.error("Failed to persist object update : " + e.getMessage(), e);
					throw new ServletException("Failed to persist object update : " + e.getMessage(), e);
				}

				response.sendRedirect("manageObjects.jsp");
			}
		} catch (Exception e) {
			//System.out.println("exception : " + e);
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("manageObjects.jsp?errorMsg=" + errorMsg).forward(request, response);
			throw new ServletException(e);
		}
	}
	
	private void updateObjectsInMetadataFile(Path dbReaderMetadataFilePath, HashMap<String, Integer> enabled,
			HashMap<String, Integer> reloadSchemaOnNextRestart, HashMap<String, Integer> reloadSchemaOnEachRestart,
			HashMap<String, Integer> reloadObjectOnNextRestart, HashMap<String, Integer> reloadObjectOnEachRestart) throws SQLException {

		String updateEnableSql = "UPDATE src_object_info SET enable = ? WHERE object_name = ?";
		String deleteReloadConfSql = "DELETE FROM src_object_reload_configurations WHERE object_name = ?";
		String insertReloadConfSql = "INSERT INTO src_object_reload_configurations(object_name, reload_schema_on_next_restart, reload_schema_on_each_restart, reload_object_on_next_restart, reload_object_on_each_restart) VALUES (?, ?, ?, ?, ?)";

		String url = "jdbc:sqlite:" + dbReaderMetadataFilePath;
		try (Connection conn = DriverManager.getConnection(url)){
			conn.setAutoCommit(false);		
			try (PreparedStatement updateEnablePstmt = conn.prepareStatement(updateEnableSql);
					PreparedStatement deleteReloadConfPstmt = conn.prepareStatement(deleteReloadConfSql);
					PreparedStatement insertReloadConfPstmt = conn.prepareStatement(insertReloadConfSql);
			) {
				for (HashMap.Entry<String, Integer> entry : enabled.entrySet()) {
					String objName = entry.getKey();
					Integer enable = entry.getValue();
					Integer rsnr = reloadSchemaOnNextRestart.get(objName);
					Integer rser = reloadSchemaOnEachRestart.get(objName);
					Integer ronr = reloadObjectOnNextRestart.get(objName);
					Integer roer = reloadObjectOnEachRestart.get(objName);
					
					updateEnablePstmt.setInt(1, enable);
					updateEnablePstmt.setString(2, objName);
					updateEnablePstmt.addBatch();
					
					deleteReloadConfPstmt.setString(1, objName);
					deleteReloadConfPstmt.addBatch();
					
					insertReloadConfPstmt.setString(1, objName);
					insertReloadConfPstmt.setInt(2, rsnr);
					insertReloadConfPstmt.setInt(3, rser);
					insertReloadConfPstmt.setInt(4, ronr);
					insertReloadConfPstmt.setInt(5, roer);
					insertReloadConfPstmt.addBatch();
				}
				updateEnablePstmt.executeBatch();
				deleteReloadConfPstmt.executeBatch();
				insertReloadConfPstmt.executeBatch();				
			}
			conn.commit();
		}
	}

	private int getNumEnabledObjects(Path dbReaderMetadataFilePath) throws SQLException {
		String url = "jdbc:sqlite:" + dbReaderMetadataFilePath;
		try (Connection conn = DriverManager.getConnection(url)){
			try (Statement stmt = conn.createStatement()) {
				try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM src_object_info WHERE enable = 1")){
					if (rs.next()) {
						return rs.getInt(1);
					}
				}
			}
		}
		return 0;
	}

	private final void initTracer(Path workDir) {
		this.globalTracer = Logger.getLogger(ManageObjects.class);    	
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

	private boolean isWindows() {
		return System.getProperty("os.name").startsWith("Windows");
	}
	
	private Integer getNumObjects(Path dbReaderMetadataFilePath) throws ServletException {
		String url = "jdbc:sqlite:" + dbReaderMetadataFilePath;
		int numObjects = 0;
		try (Connection conn = DriverManager.getConnection(url)){
			try (Statement stmt = conn.createStatement()) {
				try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM src_object_info")) {
					if (rs.next()) {
						return rs.getInt(1);
					}
				}
			}			
		} catch (Exception e) {
			throw new ServletException("Failed to read number of objects from the dbreader metadata file", e);
		}
		return numObjects;		
	}

}
