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

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * Servlet implementation class ValidateJobConfiguration
 */
@WebServlet("/validateDBTables")
public class ValidateDBTables extends HttpServlet {
	
	private class ObjectInfo {
		public String objectName;
		public String objectType;
		public String allowedColumns;
		public String uniqueKeyColumns;
		public String incrementalKeyColumns;
		public String objectGroupName;
		public Integer objectGroupPosition;
		public String maskColumns;
		public String deleteCondition;
		public String selectConditions;
		public int enableObject;
	}

	private static final long serialVersionUID = 1L;
	private Logger globalTracer;

	/**
	 * Default constructor. 
	 */
	public ValidateDBTables() {
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
		// TODO Auto-generated method stub
		String dbReaderObjectConfigurationMethodStr = "GUI";
		try {
			Path syncLiteDeviceDir = Path.of(request.getSession().getAttribute("synclite-device-dir").toString());
			Path dbreaderConfPath = syncLiteDeviceDir.resolve("synclite_dbreader.conf"); 
			Path dbreaderArgPath = syncLiteDeviceDir.resolve("synclite_dbreader.args");
			Path dbReaderMetadataFilePath = Path.of(syncLiteDeviceDir.toString(), "synclite_dbreader_metadata.db");
			Path dbReaderMetadataJsonFilePath = Path.of(syncLiteDeviceDir.toString(), "synclite_dbreader_metadata.json");
			dbReaderObjectConfigurationMethodStr = request.getSession().getAttribute("dbreader-object-configuration-method").toString();
			SrcType srcType = SrcType.valueOf(request.getSession().getAttribute("src-type").toString());
			initTracer(syncLiteDeviceDir);

			String numObjectsStr = request.getParameter("num-objects");

			JSONArray objectConfigsJsonArray = new JSONArray();
			Integer numObjects = 0;
			if (dbReaderObjectConfigurationMethodStr.equals("GUI")) {
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
						throw new ServletException("Configure DB Tables/Views page was not loaded properly. Please use Configure and Start Job workflow again.", e);
					}
				}
			} else {
				//JSON
				try {
					objectConfigsJsonArray = new JSONArray(Files.readString(dbReaderMetadataJsonFilePath));
					numObjects = objectConfigsJsonArray.length();
				} catch (Exception e) {
					throw new ServletException("Failed to load object configurations from json file : " + dbReaderMetadataJsonFilePath + ". Please use Configure and Start Job workflow again.", e);
				}
			}

			List<ObjectInfo> tiList = new ArrayList<ObjectInfo>();
			HashMap<String, HashMap<Integer, String>> objectGroupInfo = new HashMap<String, HashMap<Integer, String>>();
			
			int numEnabledObjects = 0;
			for (int i=0; i < numObjects; ++i) {
				String objectName = "";
				String objectType = "";
				String allowedColumns = "";
				String uniqueKeyColumns= "";
				String incrementalKeyColumns= "";
				String groupName = "";
				String grpPosition = "";
				String maskColumns = "";
				String deleteCondition = "";
				String selectConditions = "";
				int enableObject = 0;

				if (dbReaderObjectConfigurationMethodStr.equals("GUI")) {
					if (request.getParameter("object-name-" + i) == null) {
						throw new ServletException("Form too big to load in the GUI. Please use \"Table/View Configuration Method\" as \"JSON\" from the previous step.");
					} else {
						objectName = request.getParameter("object-name-" + i).strip();
					}
					
					if (request.getParameter("object-type-" + i) == null) {
						throw new ServletException("Form too big to load in the GUI. Please use \"Table/View Configuration Method\" as \"JSON\" from the previous step.");
					} else {
						objectType = request.getParameter("object-type-" + i).strip();
					}
					
					if (request.getParameter("allowed-columns-" + i) == null) {
						throw new ServletException("Form too big to load in the GUI. Please use \"Table/View Configuration Method\" as \"JSON\" from the previous step.");
					} else {
						allowedColumns = request.getParameter("allowed-columns-" + i).strip();
					}
					
					if (request.getParameter("unique-key-columns-" + i) == null) {
						throw new ServletException("Form too big to load in the GUI. Please use \"Table/View Configuration Method\" as \"JSON\" from the previous step.");
					} else {
						uniqueKeyColumns= request.getParameter("unique-key-columns-" + i).strip();
					}

					incrementalKeyColumns= "";
					if (request.getParameter("incremental-key-columns-" + i) != null) {
						incrementalKeyColumns= request.getParameter("incremental-key-columns-" + i).strip();
					}
					groupName = "";
					if (request.getParameter("group-name-" + i) != null) {
						groupName = request.getParameter("group-name-" + i).strip();
					} 
					grpPosition = "1";
					if (request.getParameter("group-position-" + i) != null) {
						grpPosition = request.getParameter("group-position-" + i).strip();
					}
					
					maskColumns = "";
					if (request.getParameter("mask-columns-" + i) != null) {
						maskColumns= request.getParameter("mask-columns-" + i).strip();
					}
					deleteCondition = "";
					if (request.getParameter("delete-condition-" + i) != null) {
						deleteCondition = URLDecoder.decode(request.getParameter("delete-condition-" + i), StandardCharsets.UTF_8.toString()).strip();
					}
					selectConditions = "";
					if (request.getParameter("select-conditions-" + i) != null) {
						selectConditions = URLDecoder.decode(request.getParameter("select-conditions-" + i), StandardCharsets.UTF_8.toString()).strip();
					}
					
					enableObject = 0;
					if (request.getParameter("enable-" + i) != null) {
						enableObject = 1;
						++numEnabledObjects;
					}
				} else {
					//JSON
					JSONObject objConfig = (JSONObject) objectConfigsJsonArray.get(i); 
					objectName = objConfig.getString("object_name");
					objectType = objConfig.getString("object_type");
					allowedColumns = objConfig.getJSONArray("allowed_columns").toString(1);
					uniqueKeyColumns= objConfig.getString("unique_key_columns");
					incrementalKeyColumns= objConfig.getString("incremental_key_columns");
					groupName = objConfig.getString("group_name");
					grpPosition = objConfig.getString("group_position");
					maskColumns = objConfig.getString("mask_columns");
					deleteCondition = objConfig.getString("delete_condition");
					selectConditions = objConfig.getString("select_conditions");
					try {
						enableObject = Integer.valueOf(objConfig.getString("enable"));
					} catch (Exception e) {
						throw new ServletException("Specified value " + objConfig.getString("enable") + " is not valid for object : " + objectName + ". Allowed values are 1 or 0.");
					}
					if (enableObject == 1) {
						++numEnabledObjects;
					}
				}
				
				List<String> allowedColList = new ArrayList<String>();
				List<String> allowedColDataTypeList = new ArrayList<String>();
				
				validateAndGetAllowedColList(objectName, allowedColumns, allowedColList, allowedColDataTypeList);

				List<String> uniqueKeyColList = new ArrayList<String>();
				if (! uniqueKeyColumns.isBlank()) {
					String[] tokens = uniqueKeyColumns.strip().split(",");
					for (int j=0; j < tokens.length; ++j) {
						String col = tokens[j].strip().toUpperCase();
						if (srcType == SrcType.MONGODB) {
							if (!col.equalsIgnoreCase("_id")) {
								throw new ServletException("Unique key column must be \"_id\" for source MongoDB collections");
							}
						} 
						
						if (! allowedColList.contains(col)) {
							throw new ServletException("Specified column in unique key columns : " + col + " is not present in allowed columns for object : " + objectName);
						}
						uniqueKeyColList.add(col);
					}
				}

				List<String> incrementalKeyColList = new ArrayList<String>();

				if (! incrementalKeyColumns.isBlank()) {					
					
					if (srcType == SrcType.CSV) {
						throw new ServletException("Object level incremental Key columns are not supported for CSV source. Object : " + objectName);
					}
					
					if (srcType != SrcType.MONGODB) {
						//Skip this validation for MongoDB as columns/fields are dynamic 

						String[] tokens = incrementalKeyColumns.strip().split(",");

						for (int j=0; j < tokens.length; ++j) {
							String col = tokens[j].strip().toUpperCase();
							int index = allowedColList.indexOf(col);
							if (index == -1) {
								throw new ServletException("Specified column in incremental key columns : " + col + " is not present in allowed columns for object : " + objectName);
							}
							incrementalKeyColList.add(col);
							String incrementalKeyColumnDataType = allowedColDataTypeList.get(index);
							validateIncrementalColumnDataType(objectName, col, incrementalKeyColumnDataType);
						}
					}
				}

				if (! groupName.isBlank()) {
					//check if gropName is in valid format
					if (groupName.matches("^[a-zA-Z0-9]*$")) {
						if (groupName.length() > 64) {
							throw new ServletException("Invalid object group name specified for object " + objectName + ". Object group name must be upto 64 alphanumeric characters");
						} 
					} else {
						throw new ServletException("\"Invalid object group name specified for object " + objectName + ". Object group name must contain alphanumeric characters");
					}
			
					HashMap<Integer, String> positions = objectGroupInfo.get(groupName.toUpperCase());
					if (positions == null) {
						positions = new HashMap<Integer, String>();
						objectGroupInfo.put(groupName.toUpperCase(), positions);
					}
						
					try {
						Integer pos = Integer.valueOf(grpPosition);
						if (pos == null) {
							throw new ServletException("Invalid object group position specified for object " + objectName + ". Object group position must be a valid numeric value");
						} else {
							if (pos < 1) {
								throw new ServletException("Invalid object group position specified for object " + objectName + ". Object group position must be a valid positive numeric value ");
							}
						}
						if (positions.containsKey(pos)) {
							throw new ServletException("Group position " + pos + " specified for object " + objectName + " has already been specified for some other object. Please check all group positions specified for object group " +  groupName);
						}
						positions.put(pos, objectName);
					} catch (NumberFormatException e) {
						throw new ServletException("Invalid object group position specified for object "  + objectName + ". Object group position must be a valid numeric value");
					}
				} else {
					grpPosition = "1";
				}
				
				if (! deleteCondition.isBlank()) {
					deleteCondition = deleteCondition.strip();
					if (srcType == SrcType.MONGODB) {
						//Do not validate column for mongodb as fields are dynamic
						String [] tokens = deleteCondition.split("=");
						if (tokens.length != 2) {
							throw new ServletException("Specified delete condition : " + deleteCondition + " is in an invalid format for object : " + objectName + ". Please specify a delete condition in \"FIELD_NAME eq FIELD_VALUE\" MongoDB predicate format");
						}
					} else {
						String [] tokens = deleteCondition.split("=");
						if (tokens.length != 2) {
							throw new ServletException("Specified delete condition : " + deleteCondition + " is in an invalid format for object : " + objectName + ". Please specify a delete condition in \"COL_NAME = COL_VALUE\" SQL predicate format");
						}
						String deleteKeyColumn = tokens[0].strip().toUpperCase();
						int index = allowedColList.indexOf(deleteKeyColumn);
						if (index == -1) {
							throw new ServletException("Specified delete condition : " + deleteCondition + " appears incorrect as the specified column : "  + deleteKeyColumn + " is not present in allowed columns for object : " + objectName);
					}		
				}
			}

				if (! maskColumns.isBlank()) {
					if (srcType != SrcType.MONGODB) {
						//Skip this validation for MongoDB as fields are dynamic
						//
						String[] tokens = maskColumns.split(",");
						for (int j=0; j < tokens.length; ++j) {
							String col = tokens[j].strip().toUpperCase();
							if (! allowedColList.contains(col)) {
								throw new ServletException("Specified column in mask columns : " + col + " is not present in allowed columns for object : " + objectName);
							}

							if (uniqueKeyColList.contains(col)) {
								throw new ServletException("Specified column in mask columns : " + col + " is present in primary/unique key columns for object : " + objectName + ", masking of primary/unique key columns is not supported");
							}

							if ((incrementalKeyColumns) != null && incrementalKeyColList.contains(col)) {
								throw new ServletException("Specified column in mask columns : " + col + " is spceified as an incremental key column for object : " + objectName + ", masking of incremental key column is not supported");
							}
						}
					}
				}

				if (! selectConditions.isBlank()) {
					if (srcType == SrcType.CSV) {
						throw new ServletException("Select conditions are not supported for CSV source. Object : " + objectName);
					}

					//Validate if it is a valid JSON document
					try {
				    	new JSONArray(new JSONTokener(selectConditions));
				    } catch (Exception e) {
				    	throw new ServletException("Specified select conditions are not in a valid JSON array format for object : " + objectName, e);
				    }
				}
		
				//All good

				ObjectInfo ti = new ObjectInfo();
				ti.objectName = objectName;
				ti.objectType = objectType;
				ti.allowedColumns = allowedColumns;
				ti.uniqueKeyColumns = uniqueKeyColumns;
				ti.incrementalKeyColumns = incrementalKeyColumns;
				ti.objectGroupName = groupName;
				ti.objectGroupPosition = Integer.valueOf(grpPosition);
				ti.maskColumns = maskColumns;
				ti.deleteCondition = deleteCondition;
				ti.selectConditions = selectConditions;
				ti.enableObject = enableObject;
				
				tiList.add(ti);
				
			}

			if (numEnabledObjects == 0) {
				throw new ServletException("No objects enabled. Please enable at least one object.");
			}

			//Validate tableGroupInfo
			
			validateObjectGroupInfo(objectGroupInfo);
			
			updateSrcObjectInfoInMetadataTable(dbReaderMetadataFilePath, tiList);

			request.getSession().setAttribute("num-enabled-objects", numEnabledObjects);

			/*
			//Start dbreader core job

			HttpSession session = request.getSession();
			String corePath = Path.of(getServletContext().getRealPath("/"), "WEB-INF", "lib").toString();

			//Get current job PID if running
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
				if (line.contains("com.synclite.dbreader.Main")) {
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

			//Get env variable 
			String jvmArgs = "";
			if (session.getAttribute("jvm-arguments") != null) {
				jvmArgs = session.getAttribute("jvm-arguments").toString();
			}
			//Start job again
			Process p;
			if (isWindows()) {
				String scriptName = "synclite-dbreader.bat";
				String scriptPath = Path.of(corePath, scriptName).toString();
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
				String[] cmdArray = {scriptPath.toString(), "read", "--db-dir", syncLiteDeviceDir.toString(), "--config", dbreaderConfPath.toString(), "--arguments", dbreaderArgPath.toString()};
				p = Runtime.getRuntime().exec(cmdArray);

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
						this.globalTracer.error("Failed to write jvm-arguments to synclite-dbreader-variables.sh file", e);
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

				String[] cmdArray = {scriptPath.toString(), "read", "--db-dir", syncLiteDeviceDir.toString(), "--config", dbreaderConfPath.toString(), "--arguments", dbreaderArgPath.toString()};
				p = Runtime.getRuntime().exec(cmdArray);		        	
			}
			//int exitCode = p.exitValue();
			//Thread.sleep(3000);
			Thread.sleep(5000);
			boolean processStatus = p.isAlive();
			if (!processStatus) {
				BufferedReader procErr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
				if ((line = procErr.readLine()) != null) {
					StringBuilder errorMsg = new StringBuilder();
					int i = 0;
					do {
						errorMsg.append(line);
						errorMsg.append("\n");
						line = procErr.readLine();
						if (line == null) {
							break;
						}
						++i;
					} while (i < 5);
					this.globalTracer.error("Failed to start dbreader job with exit code : " + p.exitValue() + " and errors : " + errorMsg.toString());
					throw new ServletException("Failed to start dbreader job with exit code : " + p.exitValue() + " and errors : " + errorMsg.toString());
				}

				BufferedReader procOut = new BufferedReader(new InputStreamReader(p.getInputStream()));
				if ((line = procOut.readLine()) != null) {
					StringBuilder errorMsg = new StringBuilder();
					int i = 0;
					do {
						errorMsg.append(line);
						errorMsg.append("\n");
						line = procOut.readLine();
						if (line == null) {
							break;
						}
						++i;
					} while (i < 5);

					this.globalTracer.error("Failed to start dbreader job with exit code : " + p.exitValue() + " and errors : " + errorMsg.toString());
					throw new ServletException("Failed to start dbreader job with exit code : " + p.exitValue() + " and errors : " + errorMsg.toString());
				}

				throw new ServletException("Failed to start dbreader job with exit value : " + p.exitValue());
			}
			//System.out.println("process status " + processStatus);
			//System.out.println("process output line " + line);
			request.getSession().setAttribute("job-status","STARTED");
			request.getSession().setAttribute("job-type","READ");
			request.getSession().setAttribute("job-start-time",System.currentTimeMillis());
			request.getRequestDispatcher("dashboard.jsp").forward(request, response);
			*/
			
			//request.getRequestDispatcher("confirmReadJob.jsp").forward(request, response);
			response.sendRedirect("confirmReadJob.jsp");

		} catch (Exception e) {
			//System.out.println("exception : " + e);
			this.globalTracer.error("Exception while processing request:", e);
			String errorMsg = e.getMessage();
			if (dbReaderObjectConfigurationMethodStr.equals("GUI")) {
				request.getRequestDispatcher("configureTables.jsp?errorMsg=" + errorMsg).forward(request, response);
			} else {
				request.getRequestDispatcher("configureTablesJSON.jsp?errorMsg=" + errorMsg).forward(request, response);
			}
			throw new ServletException(e);
		}
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

	private void validateObjectGroupInfo(HashMap<String, HashMap<Integer, String>> objectGroupInfo) throws ServletException {
		if (objectGroupInfo.size() == 0) {
			return;
		}		
		for (String groupName : objectGroupInfo.keySet()) {
			//For each group check if all position numbers startinvg from 1 are specified
			HashMap<Integer, String> positions = objectGroupInfo.get(groupName);
			//Check if all indexes are present 
			for (Integer pos = 1 ; pos <= positions.size(); ++pos) {
				if (!positions.containsKey(pos)) {
					throw new ServletException("No object specified for position " + pos + " for object group " + groupName + ". Please check group positions for all objects specified for object group " + groupName + ".");
				}
			}				
		}
	}

	private void validateIncrementalColumnDataType(String objectName, String incrementalKeyColumn, String incrementalKeyColumnDataType) throws ServletException {
		String dtype = incrementalKeyColumnDataType.split("[\\s(]+")[0].toLowerCase();
		switch(dtype) {
		case "date":
		case "datetime" :
		case "timestamp" :
		case "timestampz":
		case "time":
		case "timez":
		case "smalldatetime":
		case "datetime2":
		case "datetimeoffset":
		case "interval":	
        case "smallserial" :
        case "serial" :
        case "bigserial" :	
        case "bit" :
        case "integer" :
        case "int" :
        case "tinyint":
        case "smallint":
        case "mediumint":
        case "bigint":        	
        case "int2":
        case "int8":
        case "long":
        case "byteint":	
        case "unsigned":
        case "real":
        case "double":
        case "float":
        case "numeric":
        case "money":
        case "smallmoney":
        case "number":
        case "decimal":        	
        	return;
		}
		throw new ServletException("The data type for incremental key column " + incrementalKeyColumn + " specified for object " + objectName + " is " + incrementalKeyColumnDataType + ". It must be a datetime/timestamp/numeric data type.");
	}

	private void updateSrcObjectInfoInMetadataTable(Path metadataFilePath, List<ObjectInfo> tiList) throws ServletException {
		String url = "jdbc:sqlite:" + metadataFilePath;
		String metadataTableDeleteSql = "DELETE FROM src_object_info WHERE object_name = ?";
		String metadataTableInsertSql = "INSERT INTO src_object_info(object_name, object_type, allowed_columns, unique_key_columns, incremental_key_columns, group_name, group_position, mask_columns, delete_condition, select_conditions, enable) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		String metadataDisabledTablesDeleteSql = "DELETE FROM src_object_info WHERE enable = 0";

		try (Connection conn = DriverManager.getConnection(url)){
			conn.setAutoCommit(false);
			try (PreparedStatement insertPstmt = conn.prepareStatement(metadataTableInsertSql);
				PreparedStatement deletePstmt = conn.prepareStatement(metadataTableDeleteSql);) {
				for (ObjectInfo ti : tiList) {
					deletePstmt.setString(1, ti.objectName);
					deletePstmt.addBatch();
					insertPstmt.setString(1, ti.objectName);
					insertPstmt.setString(2, ti.objectType);
					insertPstmt.setString(3, ti.allowedColumns);
					insertPstmt.setString(4, ti.uniqueKeyColumns);
					insertPstmt.setString(5, ti.incrementalKeyColumns);
					insertPstmt.setString(6, ti.objectGroupName);
					insertPstmt.setInt(7, ti.objectGroupPosition);
					insertPstmt.setString(8, ti.maskColumns);
					insertPstmt.setString(9, ti.deleteCondition);
					insertPstmt.setString(10, ti.selectConditions);
					insertPstmt.setInt(11, ti.enableObject);
					insertPstmt.addBatch();				
				}
				
				if (tiList.size() > 0) {
					deletePstmt.executeBatch();
					insertPstmt.executeBatch();
				}
			}
			
			/*
			//Delete all tables which have not been enabled from metadata file. 
			try (Statement stmt = conn.createStatement()) {
				stmt.execute(metadataDisabledTablesDeleteSql);
			}
			*/
			conn.commit();
		} catch (SQLException e) {
			this.globalTracer.error("Failed to update object info in metadata file : " + metadataFilePath + " : " + e.getMessage(), e);
			throw new ServletException("Failed to update object info in metadata file : " + metadataFilePath + " : " + e.getMessage(), e);
		}
	}

	private void validateAndGetAllowedColList(String objectName, String allowedColumns, List<String> allowedColList, List<String> allowedColDataTypeList) throws ServletException {
		JSONArray tokens = null;	
		try {
			tokens = new JSONArray(new JSONTokener(allowedColumns));
		} catch (Exception e) {
			throw new ServletException("Specified allowed columns are not in a valid JSON array format for object : " + objectName + " : " + e.getMessage(), e);
		}
		for (int i = 0; i <tokens.length(); ++i) {
			String[] subTokens = null;
			String tokenToParse = tokens.get(i).toString().strip();
			if (tokenToParse.startsWith("\"")) {
				//column name is quoted , split by quote character.
				subTokens = tokenToParse.substring(1).split("\"");
			} else {
				subTokens = tokenToParse.split("\\s+", 2);
			}
			if (subTokens.length != 2) {
				throw new ServletException("Invalid value : " + tokens.get(i) + " specified for allowed columns for object : " + objectName);
			}
			allowedColList.add(subTokens[0].strip().toUpperCase());
			
			String type = subTokens[1].toLowerCase().split("not null|null", 2)[0].trim();
			allowedColDataTypeList.add(type);
		}	
	}

	private final void initTracer(Path workDir) {
		this.globalTracer = Logger.getLogger(ValidateDBTableOptions.class);    	
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

}


