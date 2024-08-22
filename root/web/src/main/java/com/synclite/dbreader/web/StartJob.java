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
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;

import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class StartJob
 */
@WebServlet("/startJob")
public class StartJob extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public StartJob() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try {
			if (request.getSession().getAttribute("synclite-device-dir") == null) {
				response.sendRedirect("syncLiteTerms.jsp");			
			} else {
				String corePath = Path.of(getServletContext().getRealPath("/"), "WEB-INF", "lib").toString();
				String syncLiteDeviceDir = request.getSession().getAttribute("synclite-device-dir").toString();
				String propsPath = Path.of(syncLiteDeviceDir, "synclite_dbreader.conf").toString();
				String dbReaderArgPath = Path.of(syncLiteDeviceDir, "synclite_dbreader.args").toString();

				boolean startWithJobArgs = false;
				if(request.getParameter("jobArgs") != null) {
					if (request.getParameter("jobArgs").equals("true")) {
						startWithJobArgs = true;
					}
				}
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
				//Start if the job is not found
				if(currentJobPID == 0) {
						//Get env variable 
						String jvmArgs = "";
						if (request.getSession().getAttribute("jvm-arguments") != null) {
							jvmArgs = request.getSession().getAttribute("jvm-arguments").toString();
						}	
						Process p;
						if (isWindows()) {
							if (!jvmArgs.isBlank()) {
								try {
									//Delete and re-create a file variables.bat under scriptPath and set the variable JVM_ARGS
									Path varFilePath = Path.of(corePath, "synclite-dbreader-variables.bat");
									if (Files.exists(varFilePath)) {
										Files.delete(varFilePath);
									}
									String varString = "set \"JVM_ARGS=" + jvmArgs + "\""; 
									Files.writeString(varFilePath, varString, StandardOpenOption.CREATE);
								} catch (Exception e) {
									throw new ServletException("Failed to write jvm-arguments to synclite-dbreader-variables.bat file", e);
								}
							}
							String scriptName = "synclite-dbreader.bat";
							String scriptPath = Path.of(corePath, scriptName).toString();

							if (startWithJobArgs) { 
								String[] cmdArray = {scriptPath, "read", "--db-dir", syncLiteDeviceDir, "--config", propsPath, "--arguments", dbReaderArgPath.toString()};
								p = Runtime.getRuntime().exec(cmdArray);						
							} else {
								String[] cmdArray = {scriptPath, "read", "--db-dir", syncLiteDeviceDir, "--config", propsPath};
								p = Runtime.getRuntime().exec(cmdArray);						
							}
						} else {
							String scriptName = "synclite-dbreader.sh";
							Path scriptPath = Path.of(corePath, scriptName);
							
							if (!jvmArgs.isBlank()) {
								try {
									//Delete and re-create a file variables.sh under scriptPath and set the variable JVM_ARGS
									Path varFilePath = Path.of(corePath, "synclite-dbreader-variables.sh");
									String varString = "JVM_ARGS=\"" + jvmArgs + "\"";
									if (Files.exists(varFilePath)) {
										Files.delete(varFilePath);
									}
									Files.writeString(varFilePath, varString, StandardOpenOption.CREATE);
									Set<PosixFilePermission> perms = Files.getPosixFilePermissions(varFilePath);
									perms.add(PosixFilePermission.OWNER_EXECUTE);
									Files.setPosixFilePermissions(varFilePath, perms);
								} catch (Exception e) {
									throw new ServletException("Failed to write jvm-arguments to synclite-dbreader-variables.sh file", e);
								}
							}
		
							// Get the current set of script permissions
							Set<PosixFilePermission> perms = Files.getPosixFilePermissions(scriptPath);
							// Add the execute permission if it is not already set
							if (!perms.contains(PosixFilePermission.OWNER_EXECUTE)) {
								perms.add(PosixFilePermission.OWNER_EXECUTE);
								Files.setPosixFilePermissions(scriptPath, perms);
							}
		
							if (startWithJobArgs) {
								String[] cmdArray = {scriptPath.toString(), "read", "--db-dir", syncLiteDeviceDir, "--config", propsPath, "--arguments", dbReaderArgPath.toString()};
								p = Runtime.getRuntime().exec(cmdArray);						
							} else {
								String[] cmdArray = {scriptPath.toString(), "read", "--db-dir", syncLiteDeviceDir, "--config", propsPath};
								p = Runtime.getRuntime().exec(cmdArray);
							}
						}
		                request.getSession().setAttribute("job-status","STARTED");
		    			request.getSession().setAttribute("job-type","READ");
		                request.getSession().setAttribute("job-start-time",System.currentTimeMillis());
		                //request.getRequestDispatcher("dashboard.jsp").forward(request, response);
						response.sendRedirect("dashboard.jsp");
				}
				else 
				{
					//request.getRequestDispatcher("dashboard.jsp").forward(request, response);
					response.sendRedirect("dashboard.jsp");
				}
			}
		} catch (Exception e) {
			String errorMsg = e.getMessage();
			//request.getRequestDispatcher("jobError.jsp?jobType=StartRead&errorMsg=" + errorMsg).forward(request, response);
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
