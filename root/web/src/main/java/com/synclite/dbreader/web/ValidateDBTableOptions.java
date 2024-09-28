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
import java.util.LinkedHashMap;
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
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Types;

/**
 * Servlet implementation class ValidateJobConfiguration
 */
@WebServlet("/validateDBTableOptions")
public class ValidateDBTableOptions extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	private Logger globalTracer;

	/**
	 * Default constructor. 
	 */
	public ValidateDBTableOptions() {
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
			Path dbReaderMetadataFilePath = Path.of(syncLiteDeviceDir.toString(), "synclite_dbreader_metadata.db");
			Path dbReaderMetadataJsonFilePath = Path.of(syncLiteDeviceDir.toString(), "synclite_dbreader_metadata.json");

			initTracer(syncLiteDeviceDir);

			String dbReaderSkipObjectConfigurationsStr = request.getParameter("dbreader-skip-object-configurations");
			String dbReaderObjectConfigurationMethodStr = request.getParameter("dbreader-object-configuration-method");
			String dbReaderConfigureIncrementalKeysStr = request.getParameter("dbreader-configure-incremental-keys");
			String dbReaderConfigureSoftDeleteConditionsStr = request.getParameter("dbreader-configure-soft-delete-conditions");
			String dbReaderConfigureMaskColumnsStr = request.getParameter("dbreader-configure-mask-columns");
			String dbReaderConfigureSelectConditionsStr = request.getParameter("dbreader-configure-select-conditions");
			String dbReaderConfigureObjectGroupsStr = request.getParameter("dbreader-configure-object-groups");

			request.getSession().setAttribute("dbreader-skip-object-configurations", dbReaderSkipObjectConfigurationsStr);
			request.getSession().setAttribute("dbreader-object-configuration-method", dbReaderObjectConfigurationMethodStr);
			request.getSession().setAttribute("dbreader-configure-incremental-keys", dbReaderConfigureIncrementalKeysStr);
			request.getSession().setAttribute("dbreader-configure-soft-delete-conditions", dbReaderConfigureSoftDeleteConditionsStr);
			request.getSession().setAttribute("dbreader-configure-mask-columns", dbReaderConfigureMaskColumnsStr);
			request.getSession().setAttribute("dbreader-configure-select-conditions", dbReaderConfigureSelectConditionsStr);
			request.getSession().setAttribute("dbreader-configure-object-groups", dbReaderConfigureObjectGroupsStr);

			if (dbReaderObjectConfigurationMethodStr.equals("JSON")) {
				//Export contents of metadata file into json file.
	            JSONArray jsonArray = new JSONArray();
				String url = "jdbc:sqlite:" + dbReaderMetadataFilePath;
				String metadataTableSelectSql = "SELECT object_name, object_type, allowed_columns, unique_key_columns, incremental_key_columns, group_name, group_position, mask_columns, delete_condition, select_conditions, enable FROM src_object_info ORDER BY object_name";
				int objIdx = 0;
				try (Connection conn = DriverManager.getConnection(url)){
					try (Statement stmt = conn.createStatement()) {
						try (ResultSet rs = stmt.executeQuery(metadataTableSelectSql)) {							
							while (rs.next()) {
				                Map<String, Object> map = new LinkedHashMap<>();
                
				                String objectName = rs.getString("object_name");
				                map.put("object_name", objectName);
				
				                String objectType= rs.getString("object_type");
				                map.put("object_type", objectType);
				
				                String allowedColumns = rs.getString("allowed_columns");
				                JSONArray allowedColumnsJSONArr = new JSONArray();
				                if (allowedColumns != null) {
				                	allowedColumnsJSONArr = new JSONArray(allowedColumns);
								} else {
									allowedColumns = "";
								}
				                map.put("allowed_columns", allowedColumnsJSONArr);
				                
				                String uniqueKeyColumns = rs.getString("unique_key_columns");
				                if (uniqueKeyColumns == null) {
				                	uniqueKeyColumns = "";
								} 
				                map.put("unique_key_columns", uniqueKeyColumns);
				                
				                String incrementalKeyColumns = rs.getString("incremental_key_columns");
				                if (incrementalKeyColumns == null) {
				                	incrementalKeyColumns = "";
								} 
				                map.put("incremental_key_columns", incrementalKeyColumns);
				                
				                String groupName = rs.getString("group_name");
				                if (groupName == null) {
				                	groupName = "";
								} 
				                map.put("group_name", groupName);
				                
				                String groupPosition= rs.getString("group_position");
				                if (groupPosition == null) {
				                	groupPosition = "1";
								} 
				                map.put("group_position", groupPosition);

				                String maskColumns = rs.getString("mask_columns");
				                if (maskColumns == null) {
				                	maskColumns = "";
								} 
				                map.put("mask_columns", maskColumns);
				                
				                String deleteCondition = rs.getString("delete_condition");
				                if (deleteCondition == null) {
				                	deleteCondition = "";
								} 
				                map.put("delete_condition", deleteCondition);
				                
				                String selectConditions = rs.getString("select_conditions");
				                if (selectConditions == null) {
				                	selectConditions = "";
								} 
				                map.put("select_conditions", selectConditions);

				                String enable = rs.getString("enable");
				                if (enable  == null) {
				                	enable  = "0";
								} 
				                map.put("enable", enable);

				                JSONObject jsonObject = new JSONObject(map);			                
				                jsonArray.put(objIdx, jsonObject);
				                ++objIdx;
				            }
						}
					}					
					Files.writeString(dbReaderMetadataJsonFilePath, jsonArray.toString(4));
				} catch (Exception e) {
					throw new ServletException("Failed to export contents of metadta db file into metadata json file : " + e.getMessage(), e);
				}
			}
			
			writeConfigurationFiles(request);

			//Next step is to fetch all object metadata

			if (dbReaderSkipObjectConfigurationsStr.equals("true")) {
				response.sendRedirect("confirmReadJob.jsp");
			} else {				
				if (dbReaderObjectConfigurationMethodStr.equals("GUI")) {
					response.sendRedirect("configureTables.jsp");
				} else {
					//JSON
					response.sendRedirect("configureTablesJSON.jsp");
				}
			}
		} catch (Exception e) {
			//System.out.println("exception : " + e);
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("configureDBTableOptions.jsp?errorMsg=" + errorMsg).forward(request, response);
			throw new ServletException(e);
		}
	}

	
	void writeConfigurationFiles(HttpServletRequest request) throws ServletException {
		//Create dbreader config string.
		String syncLiteDeviceDirStr = request.getSession().getAttribute("synclite-device-dir").toString();

		StringBuilder confBuilder = new StringBuilder();
		confBuilder.append("job-name = ").append(request.getSession().getAttribute("job-name")).append("\n");
		confBuilder.append("synclite-device-dir = ").append(request.getSession().getAttribute("synclite-device-dir")).append("\n");
		confBuilder.append("synclite-logger-configuration-file = ").append(request.getSession().getAttribute("synclite-logger-configuration-file")).append("\n");
		confBuilder.append("dbreader-trace-level = ").append(request.getSession().getAttribute("dbreader-trace-level")).append("\n");
		confBuilder.append("dbreader-enable-statistics-collector = ").append(request.getSession().getAttribute("dbreader-enable-statistics-collector")).append("\n");
		confBuilder.append("dbreader-update-statistics-interval-s = ").append(request.getSession().getAttribute("dbreader-update-statistics-interval-s")).append("\n");
		confBuilder.append("dbreader-retry-failed-objects = ").append(request.getSession().getAttribute("dbreader-retry-failed-objects")).append("\n");
		confBuilder.append("dbreader-failed-object-retry-interval-s = ").append(request.getSession().getAttribute("dbreader-failed-object-retry-interval-s")).append("\n");
		
		confBuilder.append("src-type = ").append(request.getSession().getAttribute("src-type")).append("\n");
		confBuilder.append("src-connection-string = ").append(request.getSession().getAttribute("src-connection-string")).append("\n");
		if (request.getSession().getAttribute("src-database") != null) {
			confBuilder.append("src-database = ").append(request.getSession().getAttribute("src-database")).append("\n");
		}
		if (request.getSession().getAttribute("src-schema") != null) {
			confBuilder.append("src-schema = ").append(request.getSession().getAttribute("src-schema")).append("\n");
		}			
		if (request.getSession().getAttribute("src-dblink") != null) {
			confBuilder.append("src-dblink = ").append(request.getSession().getAttribute("src-dblink")).append("\n");
		}
		if (request.getSession().getAttribute("src-user") != null) {
			confBuilder.append("src-user = ").append(request.getSession().getAttribute("src-user")).append("\n");
		}
		if (request.getSession().getAttribute("src-password") != null) {
			confBuilder.append("src-password = ").append(request.getSession().getAttribute("src-password")).append("\n");
		}

		if (request.getSession().getAttribute("src-connection-initialization-stmt") != null) {
			confBuilder.append("src-connection-initialization-stmt = ").append(request.getSession().getAttribute("src-connection-initialization-stmt")).append("\n");
		}
		
		confBuilder.append("src-connection-timeout-s = ").append(request.getSession().getAttribute("src-connection-timeout-s")).append("\n");
		confBuilder.append("src-object-type = ").append(request.getSession().getAttribute("src-object-type")).append("\n");
		confBuilder.append("src-object-name-pattern = ").append(request.getSession().getAttribute("src-object-name-pattern")).append("\n");
		confBuilder.append("src-object-metadata-read-method = ").append(request.getSession().getAttribute("src-object-metadata-read-method")).append("\n");		
		confBuilder.append("src-column-metadata-read-method = ").append(request.getSession().getAttribute("src-column-metadata-read-method")).append("\n");		
		confBuilder.append("src-constraint-metadata-read-method = ").append(request.getSession().getAttribute("src-constraint-metadata-read-method")).append("\n");		
		confBuilder.append("src-dbreader-interval-s = ").append(request.getSession().getAttribute("src-dbreader-interval-s")).append("\n");
		confBuilder.append("dbreader-stop-after-first-iteration = ").append(request.getSession().getAttribute("dbreader-stop-after-first-iteration")).append("\n");
		confBuilder.append("src-dbreader-batch-size = ").append(request.getSession().getAttribute("src-dbreader-batch-size")).append("\n");
		confBuilder.append("src-dbreader-processors = ").append(request.getSession().getAttribute("src-dbreader-processors")).append("\n");
		confBuilder.append("src-dbreader-method = ").append(request.getSession().getAttribute("src-dbreader-method")).append("\n");
		confBuilder.append("src-infer-schema-changes = ").append(request.getSession().getAttribute("src-infer-schema-changes")).append("\n");
		confBuilder.append("src-infer-object-drop = ").append(request.getSession().getAttribute("src-infer-object-drop")).append("\n");
		confBuilder.append("src-infer-object-create = ").append(request.getSession().getAttribute("src-infer-object-create")).append("\n");
		confBuilder.append("src-numeric-value-mask = ").append(request.getSession().getAttribute("src-numeric-value-mask")).append("\n");
		confBuilder.append("src-alphabetic-value-mask = ").append(request.getSession().getAttribute("src-alphabetic-value-mask")).append("\n");
		confBuilder.append("src-dbreader-object-record-limit = ").append(request.getSession().getAttribute("src-dbreader-object-record-limit")).append("\n");
		if (request.getSession().getAttribute("src-default-unique-key-column-list") != null) {
			confBuilder.append("src-default-unique-key-column-list = ").append(request.getSession().getAttribute("src-default-unique-key-column-list")).append("\n");
		}
		if (request.getSession().getAttribute("src-default-incremental-key-column-list") != null) {
			confBuilder.append("src-default-incremental-key-column-list = ").append(request.getSession().getAttribute("src-default-incremental-key-column-list")).append("\n");
		}
		if (request.getSession().getAttribute("src-default-soft-delete-condition") != null) {
			confBuilder.append("src-default-soft-delete-condition = ").append(request.getSession().getAttribute("src-default-soft-delete-condition")).append("\n");
		}
		if (request.getSession().getAttribute("src-default-mask-column-list") != null) {
			confBuilder.append("src-default-mask-column-list = ").append(request.getSession().getAttribute("src-default-mask-column-list")).append("\n");
		}
		if (request.getSession().getAttribute("src-query-timestamp-conversion-function") != null) {
			confBuilder.append("src-query-timestamp-conversion-function = ").append(request.getSession().getAttribute("src-query-timestamp-conversion-function")).append("\n");
		}
		if (request.getSession().getAttribute("src-timestamp-incremental-key-initial-value") != null) {
			confBuilder.append("src-timestamp-incremental-key-initial-value = ").append(request.getSession().getAttribute("src-timestamp-incremental-key-initial-value")).append("\n");
		}
		if (request.getSession().getAttribute("src-numeric-incremental-key-initial-value") != null) {
			confBuilder.append("src-numeric-incremental-key-initial-value = ").append(request.getSession().getAttribute("src-numeric-incremental-key-initial-value")).append("\n");
		}		
		confBuilder.append("src-reload-object-schemas-on-each-job-restart = ").append(request.getSession().getAttribute("src-reload-object-schemas-on-each-job-restart")).append("\n");
		confBuilder.append("src-reload-objects-on-each-job-restart = ").append(request.getSession().getAttribute("src-reload-objects-on-each-job-restart")).append("\n");
		confBuilder.append("src-read-null-incremental-key-records = ").append(request.getSession().getAttribute("src-read-null-incremental-key-records")).append("\n");
		confBuilder.append("src-compute-max-incremental-key-in-db = ").append(request.getSession().getAttribute("src-compute-max-incremental-key-in-db")).append("\n");		
		confBuilder.append("src-quote-object-names = ").append(request.getSession().getAttribute("src-quote-object-names")).append("\n");
		confBuilder.append("src-quote-column-names = ").append(request.getSession().getAttribute("src-quote-column-names")).append("\n");
		confBuilder.append("src-use-catalog-scope-resolution = ").append(request.getSession().getAttribute("src-use-catalog-scope-resolution")).append("\n");
		confBuilder.append("src-use-schema-scope-resolution = ").append(request.getSession().getAttribute("src-use-schema-scope-resolution")).append("\n");

		if (request.getSession().getAttribute("src-type").toString().equals("CSV")) {
			confBuilder.append("src-csv-files-with-headers = ").append(request.getSession().getAttribute("src-csv-files-with-headers")).append("\n");
			confBuilder.append("src-csv-files-field-delimiter = ").append(request.getSession().getAttribute("src-csv-files-field-delimiter")).append("\n");
			confBuilder.append("src-csv-files-record-delimiter = ").append(request.getSession().getAttribute("src-csv-files-record-delimiter")).append("\n");
			confBuilder.append("src-csv-files-escape-character = ").append(request.getSession().getAttribute("src-csv-files-escape-character")).append("\n");
			confBuilder.append("src-csv-files-quote-character = ").append(request.getSession().getAttribute("src-csv-files-quote-character")).append("\n");
			confBuilder.append("src-csv-files-null-string = ").append(request.getSession().getAttribute("src-csv-files-null-string")).append("\n");
			confBuilder.append("src-csv-files-ignore-empty-lines = ").append(request.getSession().getAttribute("src-csv-files-ignore-empty-lines")).append("\n");
			confBuilder.append("src-csv-files-trim-fields = ").append(request.getSession().getAttribute("src-csv-files-trim-fields")).append("\n");
		
		
			if (request.getSession().getAttribute("src-file-storage-type") != null) {
				confBuilder.append("src-file-storage-type = ").append(request.getSession().getAttribute("src-file-storage-type")).append("\n");
			}

			if (request.getSession().getAttribute("src-file-storage-local-fs-directory") != null) {
				confBuilder.append("src-file-storage-local-fs-directory = ").append(request.getSession().getAttribute("src-file-storage-local-fs-directory")).append("\n");
			}

			if (request.getSession().getAttribute("src-file-storage-sftp-host") != null) {
				confBuilder.append("src-file-storage-sftp-host = ").append(request.getSession().getAttribute("src-file-storage-sftp-host")).append("\n");
			}
			
			if (request.getSession().getAttribute("src-file-storage-sftp-port") != null) {
				confBuilder.append("src-file-storage-sftp-port = ").append(request.getSession().getAttribute("src-file-storage-sftp-port")).append("\n");
			}

			if (request.getSession().getAttribute("src-file-storage-sftp-directory") != null) {
				confBuilder.append("src-file-storage-sftp-directory = ").append(request.getSession().getAttribute("src-file-storage-sftp-directory")).append("\n");
			}
			
			if (request.getSession().getAttribute("src-file-storage-sftp-user") != null) {
				confBuilder.append("src-file-storage-sftp-user = ").append(request.getSession().getAttribute("src-file-storage-sftp-user")).append("\n");
			}
			
			if (request.getSession().getAttribute("src-file-storage-sftp-password") != null) {
				confBuilder.append("src-file-storage-sftp-password = ").append(request.getSession().getAttribute("src-file-storage-sftp-password")).append("\n");
			}

			if (request.getSession().getAttribute("src-file-storage-s3-url") != null) {
				confBuilder.append("src-file-storage-s3-url = ").append(request.getSession().getAttribute("src-file-storage-s3-url")).append("\n");
			}

			if (request.getSession().getAttribute("src-file-storage-s3-bucket-name") != null) {
				confBuilder.append("src-file-storage-s3-bucket-name = ").append(request.getSession().getAttribute("src-file-storage-s3-bucket-name")).append("\n");
			}
			
			if (request.getSession().getAttribute("src-file-storage-s3-access-key") != null) {
				confBuilder.append("src-file-storage-s3-access-key = ").append(request.getSession().getAttribute("src-file-storage-s3-access-key")).append("\n");
			}

			if (request.getSession().getAttribute("src-file-storage-s3-secret-key") != null) {
				confBuilder.append("src-file-storage-s3-secret-key = ").append(request.getSession().getAttribute("src-file-storage-s3-secret-key")).append("\n");
			}
		}
		
		confBuilder.append("dbreader-skip-object-configurations = ").append(request.getSession().getAttribute("dbreader-skip-object-configurations")).append("\n");
		confBuilder.append("dbreader-object-configuration-method = ").append(request.getSession().getAttribute("dbreader-object-configuration-method")).append("\n");
		confBuilder.append("dbreader-configure-incremental-keys = ").append(request.getSession().getAttribute("dbreader-configure-incremental-keys")).append("\n");
		confBuilder.append("dbreader-configure-soft-delete-conditions = ").append(request.getSession().getAttribute("dbreader-configure-soft-delete-conditions")).append("\n");
		confBuilder.append("dbreader-configure-mask-columns = ").append(request.getSession().getAttribute("dbreader-configure-mask-columns")).append("\n");
		confBuilder.append("dbreader-configure-select-conditions = ").append(request.getSession().getAttribute("dbreader-configure-select-conditions")).append("\n");
		confBuilder.append("dbreader-configure-object-groups = ").append(request.getSession().getAttribute("dbreader-configure-object-groups")).append("\n");
		
		
		String dbReaderConfPath = Path.of(syncLiteDeviceDirStr, "synclite_dbreader.conf").toString();

		try {
			Files.writeString(Path.of(dbReaderConfPath), confBuilder.toString());
		} catch (IOException e) {
			this.globalTracer.error("Failed to write SyncLite dbreader configurations into file : " + dbReaderConfPath, e);	
			throw new ServletException("Failed to write SyncLite dbreader configurations into file : " + dbReaderConfPath, e);
		}

		StringBuilder argBuilder = new StringBuilder();
		argBuilder.append("src-reload-object-schemas = ").append(request.getSession().getAttribute("src-reload-object-schemas")).append("\n");
		argBuilder.append("src-reload-objects = ").append(request.getSession().getAttribute("src-reload-objects")).append("\n");

		String dbReaderArgPath = Path.of(syncLiteDeviceDirStr, "synclite_dbreader.args").toString();

		try {
			Files.writeString(Path.of(dbReaderArgPath), argBuilder.toString());
		} catch (IOException e) {
			this.globalTracer.error("Failed to write SyncLite dbreader arguments into file : " + dbReaderArgPath, e);	
			throw new ServletException("Failed to write SyncLite dbreader arguments into file : " + dbReaderArgPath, e);
		}

		//Now write out SyncLite logger configurations
		String syncliteLoggerConfigFileStr = request.getSession().getAttribute("synclite-logger-configuration-file").toString();

		try {
			Files.writeString(Path.of(syncliteLoggerConfigFileStr), request.getSession().getAttribute("synclite-logger-configuration").toString());
		} catch (IOException e) {
			this.globalTracer.error("Failed to write SyncLite logger configurations into file : " + syncliteLoggerConfigFileStr, e);
			throw new ServletException("Failed to write SyncLite logger configurations into file : " + syncliteLoggerConfigFileStr, e);
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

}
