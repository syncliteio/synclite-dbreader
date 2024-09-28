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


import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.bson.Document;
import org.json.JSONArray;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.AwsHostNameUtils;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpStatVFS;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

/**
 * Servlet implementation class ValidateJobConfiguration
 */
@WebServlet("/validateDBReader")
public class ValidateDBReader extends HttpServlet {

	private class ObjectInfo {
		public String objectName;
		public String objectType;
		public String columnList;
		public String pkColList;
		public boolean reloadObjectSchemas;
	}

	private class SrcInfo {
		public SrcType type;
		public String connectionString;
		public String catalog;
		public String schema;
		public String dbLink;
		public String user;
		public String password;
		public long connectionTimeoutMs; 
		public String objectType;
		public String objectNamePattern;
		public CSVFormat csvFormat;
		public boolean hasColumnMetadata;
	}

	private static final long serialVersionUID = 1L;
	private Logger globalTracer;

	/**
	 * Default constructor. 
	 */
	public ValidateDBReader() {
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

			Class.forName("org.sqlite.JDBC");

			String jobName = request.getSession().getAttribute("job-name").toString();
			String syncLiteDeviceDirStr = request.getSession().getAttribute("synclite-device-dir").toString();
			Path syncLiteDeviceDir;
			if ((syncLiteDeviceDirStr== null) || syncLiteDeviceDirStr.trim().isEmpty()) {
				throw new ServletException("\"SyncLite Device Directory Path\" must be specified");
			} else {
				syncLiteDeviceDir = Path.of(syncLiteDeviceDirStr);
				if (! Files.exists(syncLiteDeviceDir)) {
					Files.createDirectories(syncLiteDeviceDir);
				}

				if (! Files.exists(syncLiteDeviceDir)) {
					throw new ServletException("Specified \"SyncLite Device Dir Path\" : " + syncLiteDeviceDir + " does not exist, please specify a valid path.");
				}
			}
			
			//If syncLiteDeviceDir is set to default path then create default stageDir and commandDir as well.			
			String defaultSyncLiteDeviceDirStr = Path.of(System.getProperty("user.home"), "synclite", jobName, "db").toString();
			if (syncLiteDeviceDirStr.equals(defaultSyncLiteDeviceDirStr)) {
				//Also try creating stageDir and commandDir here itself
				Path defaultSyncLiteStageDir = Path.of(System.getProperty("user.home"), "synclite", jobName, "stageDir");
				Path defaultSyncLiteCommandDir = Path.of(System.getProperty("user.home"), "synclite", jobName, "commandDir");
				
				try {
					Files.createDirectories(defaultSyncLiteStageDir);
				} catch (Exception e) {
					throw new ServletException("Failed to create stage directory : " + defaultSyncLiteStageDir + " : " + e.getMessage(), e); 
				}
				
				try {
					Files.createDirectories(defaultSyncLiteCommandDir);
				} catch (Exception e) {
					throw new ServletException("Failed to create stage directory : " + defaultSyncLiteStageDir + " : " + e.getMessage(), e); 
				}				
			}
			
			initTracer(syncLiteDeviceDir);

			String syncliteLoggerConfigFileStr = request.getParameter("synclite-logger-configuration-file");
			Path loggerConfigPath;
			if ((syncliteLoggerConfigFileStr == null) || syncliteLoggerConfigFileStr.trim().isEmpty()) {
				throw new ServletException("\"SyncLite Logger Configuration File\" must be specified");
			} else {
				loggerConfigPath = Path.of(syncliteLoggerConfigFileStr);
				Path loggerConfigFileParent= Path.of(syncliteLoggerConfigFileStr).getParent();
				if (! Files.exists(loggerConfigFileParent)) {
					throw new ServletException("Specified \"SyncLite Logger Config File Path\" : " + syncliteLoggerConfigFileStr + " does not exist");
				}
			}

			String syncLiteLoggerConf = request.getParameter("synclite-logger-configuration");
			if ((syncLiteLoggerConf == null) || syncLiteLoggerConf.trim().isEmpty()) {
				throw new ServletException("\"SyncLite Logger Configurations\" must be specified");
			}

			String srcTypeStr = request.getParameter("src-type");
			SrcType srcType = SrcType.valueOf(srcTypeStr);

			String srcConnectionString = request.getParameter("src-connection-string");

			if (!srcTypeStr.equals("NONE") && (srcType != SrcType.CSV)) {
				if ((srcConnectionString == null) || srcConnectionString.trim().isEmpty()) {
					throw new ServletException("\"Source DB Connection String (JDBC Connection URL)\" must be specified");
				}
			}

			String srcDatabase = null;
			if (request.getParameter("src-database") != null) {
				if (!request.getParameter("src-database").isBlank()) {
					srcDatabase = request.getParameter("src-database").strip();

				} else {
					if (isDatabaseAllowed(srcType)) {
						throw new ServletException("You must specify Source DB Catalog/Database");
					}
				}
			} else {
				if (isDatabaseAllowed(srcType)) {
					throw new ServletException("You must specify Source DB Catalog/Database");
				}
			}

			String srcSchema = null;
			if (request.getParameter("src-schema") != null) {
				if (!request.getParameter("src-schema").isBlank()) {
					srcSchema = request.getParameter("src-schema").strip();
				} else {
					if (isSchemaAllowed(srcType)) {
						throw new ServletException("You must specify Source DB Schema");
					}
				}				
			} else {
				if (isSchemaAllowed(srcType)) {
					throw new ServletException("You must specify Source DB Schema");
				}
			}

			String srcDBLink = null;
			if (request.getParameter("src-dblink") != null) {
				if (!request.getParameter("src-dblink").isBlank()) {
					srcDBLink = request.getParameter("src-dblink").strip();
				}
			}


			String srcUser = null;
			if (request.getParameter("src-user") != null) {
				if (!request.getParameter("src-user").isBlank()) {
					srcUser = request.getParameter("src-user");
				}
			}

			String srcPassword = null;
			if (request.getParameter("src-password") != null) {
				if (!request.getParameter("src-password").isBlank()) {
					srcPassword = request.getParameter("src-password");
				}
			}

			String srcConnectionTimeoutSStr = request.getParameter("src-connection-timeout-s");
			long srcConnectionTimeoutMs = 30000L;
			try {
				if (Long.valueOf(srcConnectionTimeoutSStr) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Source DB Connection Timeout\"");
				} else if (Long.valueOf(srcConnectionTimeoutSStr) <= 0) {
					throw new ServletException("Please specify a positive numeric value for \"Source DB Connection Timeout\"");
				}
				srcConnectionTimeoutMs = Long.valueOf(srcConnectionTimeoutSStr) * 1000;
			} catch (NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Source DB Connection Timeout\"");
			}

			String jvmArguments = null;
			if (request.getParameter("jvm-arguments") != null) {
				if (!request.getParameter("jvm-arguments").isBlank()) {
					jvmArguments = request.getParameter("jvm-arguments");
				}
			}

			String srcDBReaderIntervalStr = request.getParameter("src-dbreader-interval-s");
			try {
				if (Long.valueOf(srcDBReaderIntervalStr) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Source DB Reader Interval\"");
				} else if (Long.valueOf(srcDBReaderIntervalStr) <= 0) {
					throw new ServletException("Please specify a positive numeric value for \"Source DB Reader Interval\"");
				}
			} catch (NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Source DB Reader Interval\"");
			}

			String dbReaderStopAfterFirstIteration = request.getParameter("dbreader-stop-after-first-iteration");

			String srcDBReaderBatchSizeStr = request.getParameter("src-dbreader-batch-size");
			try {
				if (Long.valueOf(srcDBReaderBatchSizeStr) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Source DB Reader Batch Size\"");
				} else if (Long.valueOf(srcDBReaderBatchSizeStr) <= 0) {
					throw new ServletException("Please specify a positive numeric value for \"Source DB Reader Batch Size\"");
				}
			} catch (NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Source DB Reader Batch Size\"");
			}

			String srcDBReaderProcessorsStr = request.getParameter("src-dbreader-processors");
			try {
				if (Integer.valueOf(srcDBReaderProcessorsStr) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Source DB Reader Processors\"");
				} else if (Integer.valueOf(srcDBReaderProcessorsStr) <= 0) {
					throw new ServletException("Please specify a positive numeric value for \"Source DB Reader Processors\"");
				}
			} catch (NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Source DB Reader Processors\"");
			}
			
			String srcDBReaderMethodStr = request.getParameter("src-dbreader-method");
			boolean validateLogBasedReplication = false;
			if (srcDBReaderMethodStr.equals("LOG_BASED")) {
				if (srcType == SrcType.MONGODB) {
					validateLogBasedReplication = true;
				} else {
					throw new ServletException("\"Source DB Reader Method\"" + srcDBReaderMethodStr + " not supported for source type : " + srcType);
				}
			}

			String srcInferSchemaChangesStr = request.getParameter("src-infer-schema-changes");
			String srcInferObjectDropStr = request.getParameter("src-infer-object-drop");
			String srcInferObjectCreateStr = request.getParameter("src-infer-object-create");

			String srcReloadObjectSchemasStr = request.getParameter("src-reload-object-schemas");
			String srcReloadObjectsSchemasOnEachJobRestartStr = request.getParameter("src-reload-object-schemas-on-each-job-restart");

			String srcReloadObjectsStr = request.getParameter("src-reload-objects");
			String srcReloadObjectsOnEachJobRestartStr = request.getParameter("src-reload-objects-on-each-job-restart");

			String srcReadNullIncrementalKeyRecordsStr = request.getParameter("src-read-null-incremental-key-records");
			
			String srcComputeMaxIncrementalKeyInDBStr = request.getParameter("src-compute-max-incremental-key-in-db");

			String srcQuoteObjectNamesStr = request.getParameter("src-quote-object-names");

			String srcQuoteColumnNamesStr = request.getParameter("src-quote-column-names");

			String srcUseCatalogScopeResolution = request.getParameter("src-use-catalog-scope-resolution");

			String srcUseSchemaScopeResolution = request.getParameter("src-use-schema-scope-resolution");

			boolean reloadObjectSchemas = Boolean.valueOf(srcReloadObjectSchemasStr) || 
					Boolean.valueOf(srcReloadObjectsSchemasOnEachJobRestartStr) || 
					Boolean.valueOf(srcReloadObjectsStr) || 
					Boolean.valueOf(srcReloadObjectsOnEachJobRestartStr);

			String srcObjectType = request.getParameter("src-object-type");

			String srcObjectNamePattern = request.getParameter("src-object-name-pattern");
			if((srcObjectNamePattern) == null || (srcObjectNamePattern.isBlank())) {
				throw new ServletException("Please specify a valid \"Source DB Object Name Pattern\"");				
			}

			String srcObjectMetadataReadMethod = request.getParameter("src-object-metadata-read-method");
			String srcColumnMetadataReadMethod = request.getParameter("src-column-metadata-read-method");
			String srcConstraintMetadataReadMethod = request.getParameter("src-constraint-metadata-read-method");

			String srcNumericValueMask = request.getParameter("src-numeric-value-mask");
			if (srcNumericValueMask != null) {
				if (srcNumericValueMask.length() != 1) {
					throw new ServletException("Please specify a single digit value for \"Source DB Numeric Value Mask\"");
				}
				if (! srcNumericValueMask.matches("[0-9]")) {
					throw new ServletException("Please specify a single digit value for \"Source DB Numeric Value Mask\"");
				}
			} else {
				srcNumericValueMask = "9";
			}

			String srcAlphabeticValueMask = request.getParameter("src-alphabetic-value-mask");
			if (srcAlphabeticValueMask != null) {
				if (srcAlphabeticValueMask.length() != 1) {
					throw new ServletException("Please specify a single alphabetic character value for \"Source DB Alphabetic Value Mask\"");
				}
				if (! srcAlphabeticValueMask.matches("[a-zA-Z]")) {
					throw new ServletException("Please specify a single alphabetic character value for \"Source DB Alphabetic Value Mask\"");
				}
			} else {
				srcAlphabeticValueMask = "X";
			}

			String srcDBReaderObjectRecordLimitStr = request.getParameter("src-dbreader-object-record-limit");
			try {
				if (Long.valueOf(srcDBReaderObjectRecordLimitStr) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Source DB Object Record Limit\"");
				} else if (Long.valueOf(srcDBReaderObjectRecordLimitStr) < 0) {
					throw new ServletException("Please specify a non-negative numeric value for \"Source DB Object Record Limit\"");
				}
			} catch (NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Source DB Object Record Limit\"");
			}

			switch(srcType) {	
			case DUCKDB:
				String srcTypeName = "DuckDB";
				String jdbcPrefix = "jdbc:duckdb:";
				int lastIndex = srcConnectionString.length();
				if (srcConnectionString.lastIndexOf("?") > 0) {
					lastIndex = srcConnectionString.lastIndexOf("?");
				}
				Path dbPath = Path.of(srcConnectionString.substring(jdbcPrefix.length(), lastIndex));
				validateDBPath(dbPath);
				break;

			case MONGODB:
				srcTypeName = "MongoDB";
				validateMongoDBConnection(srcConnectionString, srcDatabase, validateLogBasedReplication);
				break;

			case MYSQL:	
				srcTypeName = "MySQL";
				Class.forName("com.mysql.cj.jdbc.Driver");
				validateConnection(srcConnectionString, srcUser, srcPassword);
				break;

			case POSTGRESQL:	
				srcTypeName = "PostgreSQL";
				Class.forName("org.postgresql.Driver");
				validateConnection(srcConnectionString, srcUser, srcPassword);
				break;

			case SQLITE:
				srcTypeName = "SQLite";
				jdbcPrefix = "jdbc:sqlite:";
				lastIndex = srcConnectionString.length();
				if (srcConnectionString.lastIndexOf("?") > 0) {
					lastIndex = srcConnectionString.lastIndexOf("?");
				}
				dbPath = Path.of(srcConnectionString.substring(jdbcPrefix.length(), lastIndex));
				validateDBPath(dbPath);
				break;

			case CSV:
				//Do nothing
				break;
			default:
				throw new ServletException("Unsupported source type : " + srcTypeStr);
			}

			String srcConnectionInitializationStatement = null;
			if (request.getParameter("src-connection-initialization-stmt") != null) {
				if (!request.getParameter("src-connection-initialization-stmt").isBlank()) {
					srcConnectionInitializationStatement = request.getParameter("src-connection-initialization-stmt").strip();
				}
			}

			String dbReaderTraceLevel = request.getParameter("dbreader-trace-level");
			String dbReaderEnableStatisticsCollectorStr = request.getParameter("dbreader-enable-statistics-collector");
			String dbReaderUpdateStatisticsIntervalS = request.getParameter("dbreader-update-statistics-interval-s");
			try {
				if (Long.valueOf(dbReaderUpdateStatisticsIntervalS) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Statistics Update Interval(s)\"");
				} else if (Long.valueOf(dbReaderUpdateStatisticsIntervalS) <= 0) {
					throw new ServletException("Please specify a positive numeric value for \"Statistics Update Interval(s)\"");
				}
			} catch (NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Statistics Update Interval(s)\"");
			}

			String dbReaderRetryFailedObjects = request.getParameter("dbreader-retry-failed-objects");			
			String dbReaderFailedObjectRetryIntervalS = request.getParameter("dbreader-failed-object-retry-interval-s");
			try {
				if (Long.valueOf(dbReaderFailedObjectRetryIntervalS) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Failed Object Retry Interval(s)\"");
				} else if (Long.valueOf(dbReaderFailedObjectRetryIntervalS) <= 0) {
					throw new ServletException("Please specify a positive numeric value for \"Failed Object Retry Interval(s)\"");
				}
			} catch (NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Failed Object Retry Interval(s)\"");
			}

			String srcDefaultUniqueKeyColumnList = request.getParameter("src-default-unique-key-column-list");
			if (srcDefaultUniqueKeyColumnList != null) {
				srcDefaultUniqueKeyColumnList = srcDefaultUniqueKeyColumnList.strip();
				if (! srcDefaultUniqueKeyColumnList.isBlank()) {
					if (srcType == SrcType.MONGODB) {
						if (! srcDefaultUniqueKeyColumnList.equals("_id")) {
							throw new ServletException("Default Unique Key column must be \"_id\" for MongoDB source");
						}
					}
				}
			}

			String srcDefaultIncrementalKeyColumnList = request.getParameter("src-default-incremental-key-column-list");
			if ((srcDefaultIncrementalKeyColumnList != null) && !srcDefaultIncrementalKeyColumnList.isBlank()) {
				if (srcType == SrcType.CSV) {
					//Default incremental key for CSV must be FILE_CREATION_TIME					
					if (! srcDefaultIncrementalKeyColumnList.equals("FILE_CREATION_TIME")) {
						throw new ServletException("Default Incremental Key column must be FILE_CREATION_TIME for CSV source");
					}
				}
				srcDefaultIncrementalKeyColumnList = srcDefaultIncrementalKeyColumnList.strip();
			}

			String srcDefaultSoftDeleteCondition = request.getParameter("src-default-soft-delete-condition");
			if (srcDefaultSoftDeleteCondition != null) {
				srcDefaultSoftDeleteCondition = srcDefaultSoftDeleteCondition.strip();
				if (!srcDefaultSoftDeleteCondition.isBlank()) {
					String [] tokens = srcDefaultSoftDeleteCondition.split("=");
					if (tokens.length != 2) {
						throw new ServletException("Specified default soft delete condition : " + srcDefaultSoftDeleteCondition + " is in an invalid format for object : Please specify a valid soft delete condition in \"COL_NAME = COL_VALUE\" SQL predicate format");
					}
				}
			}

			String srcDefaultMaskColumnList = request.getParameter("src-default-mask-column-list");
			if (srcDefaultMaskColumnList != null) {
				srcDefaultMaskColumnList = srcDefaultMaskColumnList.strip();
				if (srcType == SrcType.MONGODB) {
					if (srcDefaultMaskColumnList.contains("_id")) {
						throw new ServletException("Specified default mask column list contains _id field which cannot be masked");
					}
				}
			}

			String srcQureyTimestampConversionFunction = null;
			if (request.getParameter("src-query-timestamp-conversion-function") != null) {
				if (!request.getParameter("src-query-timestamp-conversion-function").isBlank()) {
					srcQureyTimestampConversionFunction = request.getParameter("src-query-timestamp-conversion-function").strip();
				}
			}

			String srcTimestampIncrementalKeyInitialValue = null;
			if (request.getParameter("src-timestamp-incremental-key-initial-value") != null) {
				if (!request.getParameter("src-timestamp-incremental-key-initial-value").isBlank()) {
					srcTimestampIncrementalKeyInitialValue = request.getParameter("src-timestamp-incremental-key-initial-value").strip();
				}
			}

			String srcNumericIncrementalKeyInitialValue = null;
			if (request.getParameter("src-numeric-incremental-key-initial-value") != null) {
				if (! request.getParameter("src-numeric-incremental-key-initial-value").isBlank()) {
					srcNumericIncrementalKeyInitialValue = request.getParameter("src-numeric-incremental-key-initial-value").strip();
				}
			}

			CSVFormat csvFormat = CSVFormat.DEFAULT;
			boolean hasColumnMetadata = true;
			if (srcType == SrcType.CSV) {

				String srcFileStorageType = request.getParameter("src-file-storage-type");

				switch(srcFileStorageType) {
				case "LOCAL_FS":
					String srcFileStorageLocalFSDirectory = request.getParameter("src-file-storage-local-fs-directory");
					if (srcFileStorageLocalFSDirectory.isBlank()) {
						throw new ServletException("\"Local File System Directory\" cannot be blank");
					}
					if (!Files.exists(Path.of(srcFileStorageLocalFSDirectory))) {
						throw new ServletException("Specified \"Local File System Directory\" does not exist : " + srcFileStorageLocalFSDirectory);
					}

					if (!Path.of(srcFileStorageLocalFSDirectory).toFile().canRead()) {
						throw new ServletException("Specified \"Local File system Directory\" does not have read access : " + srcFileStorageLocalFSDirectory);
					}
					request.getSession().setAttribute("src-file-storage-local-fs-directory", srcFileStorageLocalFSDirectory);
					break;

				case "SFTP":
					srcFileStorageLocalFSDirectory = request.getParameter("src-file-storage-local-fs-directory");
					if (srcFileStorageLocalFSDirectory.isBlank()) {
						throw new ServletException("\"Local File System Directory\" cannot be blank");
					}
					if (!Files.exists(Path.of(srcFileStorageLocalFSDirectory))) {
						throw new ServletException("Specified \"Local File System Directory\" does not exist : " + srcFileStorageLocalFSDirectory);
					}

					if (!Path.of(srcFileStorageLocalFSDirectory).toFile().canRead()) {
						throw new ServletException("Specified \"Local File system Directory\" does not have read access : " + srcFileStorageLocalFSDirectory);
					}
					if (!Path.of(srcFileStorageLocalFSDirectory).toFile().canWrite()) {
						throw new ServletException("Specified \"Local File system Directory\" does not have write access : " + srcFileStorageLocalFSDirectory);
					}

					String srcFileStorageSFTPHost = request.getParameter("src-file-storage-sftp-host");
					if (srcFileStorageSFTPHost.isBlank()) {
						throw new ServletException("\"Source SFTP Host\" cannot be blank");
					}
					String srcFileStorageSFTPPort = request.getParameter("src-file-storage-sftp-port");
					if (srcFileStorageSFTPPort.isBlank()) {
						throw new ServletException("\"Source SFTP Port\" cannot be blank");
					}
					try {
						Integer port = Integer.valueOf(srcFileStorageSFTPPort);
						if (port <= 0) {
							throw new ServletException("Please enter a valid numeric value for \"Source SFTP Port\"");
						}
					} catch(IllegalArgumentException e) {
						throw new ServletException("Please enter a valid numeric \"Source SFTP Port\"");
					}
					String srcFileStorageSFTPDirectory = request.getParameter("src-file-storage-sftp-directory");
					if (srcFileStorageSFTPDirectory.isBlank()) {
						throw new ServletException("\"Source SFTP Directory\" cannot be blank");
					}

					String srcFileStorageSFTPUser = request.getParameter("src-file-storage-sftp-user");
					if (srcFileStorageSFTPUser.isBlank()) {
						throw new ServletException("\"Source SFTP User\" cannot be blank");
					}

					String srcFileStorageSFTPPassword = request.getParameter("src-file-storage-sftp-password");
					if (srcFileStorageSFTPPassword.isBlank()) {
						throw new ServletException("\"Source SFTP User Password\" cannot be blank");
					}

					request.getSession().setAttribute("src-file-storage-local-fs-directory", srcFileStorageLocalFSDirectory);
					request.getSession().setAttribute("src-file-storage-sftp-host", srcFileStorageSFTPHost);
					request.getSession().setAttribute("src-file-storage-sftp-port", srcFileStorageSFTPPort);
					request.getSession().setAttribute("src-file-storage-sftp-directory", srcFileStorageSFTPDirectory);
					request.getSession().setAttribute("src-file-storage-sftp-user", srcFileStorageSFTPUser);
					request.getSession().setAttribute("src-file-storage-sftp-password", srcFileStorageSFTPPassword);
					break;

				case "S3":

					srcFileStorageLocalFSDirectory = request.getParameter("src-file-storage-local-fs-directory");
					if (srcFileStorageLocalFSDirectory.isBlank()) {
						throw new ServletException("\"Source Local File System Directory\" cannot be blank");
					}
					if (!Files.exists(Path.of(srcFileStorageLocalFSDirectory))) {
						throw new ServletException("Specified \"Source Local File System Directory\" does not exist : " + srcFileStorageLocalFSDirectory);
					}

					if (!Path.of(srcFileStorageLocalFSDirectory).toFile().canRead()) {
						throw new ServletException("Specified \"Source Local File system Directory\" does not have read access : " + srcFileStorageLocalFSDirectory);
					}
					if (!Path.of(srcFileStorageLocalFSDirectory).toFile().canWrite()) {
						throw new ServletException("Specified \"Source Local File system Directory\" does not have write access : " + srcFileStorageLocalFSDirectory);
					}

					String srcFileStorageS3Url = request.getParameter("src-file-storage-s3-url");
					if (srcFileStorageS3Url.isBlank()) {
						throw new ServletException("\"Source S3 URL\" cannot be blank");
					}

					String srcFileStorageS3BucketName = request.getParameter("src-file-storage-s3-bucket-name");
					if (srcFileStorageS3BucketName.isBlank()) {
						throw new ServletException("\"Source S3 Bucket Name\" cannot be blank");
					}

					String srcFileStorageS3AccessKey = request.getParameter("src-file-storage-s3-access-key");
					if (srcFileStorageS3AccessKey.isBlank()) {
						throw new ServletException("\"Source S3 Access Key\" cannot be blank");
					}

					String srcFileStorageS3SecretKey = request.getParameter("src-file-storage-s3-secret-key");
					if (srcFileStorageS3SecretKey.isBlank()) {
						throw new ServletException("\"Source S3 Secret Key\" cannot be blank");
					}

					request.getSession().setAttribute("src-file-storage-local-fs-directory", srcFileStorageLocalFSDirectory);				
					request.getSession().setAttribute("src-file-storage-s3-url", srcFileStorageS3Url);
					request.getSession().setAttribute("src-file-storage-s3-bucket-name", srcFileStorageS3BucketName);
					request.getSession().setAttribute("src-file-storage-s3-access-key", srcFileStorageS3AccessKey);
					request.getSession().setAttribute("src-file-storage-s3-secret-key", srcFileStorageS3SecretKey);
					break;					
				}

				request.getSession().setAttribute("src-file-storage-type", srcFileStorageType);

				//Get and validate csv options.
				String srcCsvFilesWithHeader = request.getParameter("src-csv-files-with-headers");
				if (srcCsvFilesWithHeader.equals("true")) {
					hasColumnMetadata = true;
				} else {
					hasColumnMetadata = false;
				}
				String srcCsvFilesFieldDelimiter = request.getParameter("src-csv-files-field-delimiter");
				if (srcCsvFilesFieldDelimiter.isBlank()) {
					throw new ServletException("\"Source CSV File Field Delimiter\" cannot be blank");
				}
				if (srcCsvFilesFieldDelimiter.length() > 1) {
					throw new ServletException("\"Source CSV File Field Delimiter\" must be a single character");
				}
				csvFormat = csvFormat.withDelimiter(srcCsvFilesFieldDelimiter.charAt(0));

				String srcCsvFilesRecordDelimiter = request.getParameter("src-csv-files-record-delimiter");
				if (srcCsvFilesRecordDelimiter.isBlank()) {
					throw new ServletException("\"Source CSV File Record Delimiter\" cannot be blank");
				}
				csvFormat = csvFormat.withRecordSeparator(srcCsvFilesRecordDelimiter);

				String srcCsvFilesEscapeCharacter = request.getParameter("src-csv-files-escape-character");
				if (srcCsvFilesEscapeCharacter.isBlank()) {
					throw new ServletException("\"Source CSV File Escape Character\" cannot be blank");
				}			
				if (srcCsvFilesEscapeCharacter.length() > 1) {
					throw new ServletException("\"Source CSV File Escape Character\" must be a single character");
				}
				csvFormat = csvFormat.withEscape(srcCsvFilesEscapeCharacter.charAt(0));

				String srcCsvFilesQuoteCharacter = request.getParameter("src-csv-files-quote-character");
				if (srcCsvFilesQuoteCharacter.isBlank()) {
					throw new ServletException("\"Source CSV File Quote Character\" cannot be blank");
				}
				if (srcCsvFilesEscapeCharacter.length() > 1) {
					throw new ServletException("\"Source CSV File Quote Character\" must be a single character");
				}
				csvFormat = csvFormat.withQuote(srcCsvFilesQuoteCharacter.charAt(0));

				String srcCsvFilesNullString = request.getParameter("src-csv-files-null-string");
				csvFormat = csvFormat.withNullString(srcCsvFilesNullString);

				String srcCsvFilesIgnoreEmptyLines = request.getParameter("src-csv-files-ignore-empty-lines");
				if (srcCsvFilesIgnoreEmptyLines.equals("true")) {
					csvFormat = csvFormat.withIgnoreEmptyLines();
				}
				String srcCsvFilesTrimFields = request.getParameter("src-csv-files-trim-fields");
				if (srcCsvFilesTrimFields.equals("true")) {
					csvFormat = csvFormat.withTrim();
				}

				request.getSession().setAttribute("src-csv-files-with-headers", srcCsvFilesWithHeader);
				request.getSession().setAttribute("src-csv-files-field-delimiter", srcCsvFilesFieldDelimiter);
				request.getSession().setAttribute("src-csv-files-record-delimiter", srcCsvFilesRecordDelimiter);
				request.getSession().setAttribute("src-csv-files-escape-character", srcCsvFilesEscapeCharacter);
				request.getSession().setAttribute("src-csv-files-quote-character", srcCsvFilesQuoteCharacter);
				request.getSession().setAttribute("src-csv-files-null-string", srcCsvFilesNullString);
				request.getSession().setAttribute("src-csv-files-ignore-empty-lines", srcCsvFilesIgnoreEmptyLines);
				request.getSession().setAttribute("src-csv-files-trim-fields", srcCsvFilesTrimFields);				
			}

			request.getSession().setAttribute("synclite-device-dir", syncLiteDeviceDirStr);
			request.getSession().setAttribute("synclite-logger-configuration-file", syncliteLoggerConfigFileStr);
			request.getSession().setAttribute("synclite-logger-configuration", syncLiteLoggerConf);
			request.getSession().setAttribute("dbreader-trace-level", dbReaderTraceLevel);
			request.getSession().setAttribute("dbreader-enable-statistics-collector", dbReaderEnableStatisticsCollectorStr);
			request.getSession().setAttribute("dbreader-update-statistics-interval-s", dbReaderUpdateStatisticsIntervalS);
			request.getSession().setAttribute("dbreader-retry-failed-objects", dbReaderRetryFailedObjects);
			request.getSession().setAttribute("dbreader-failed-object-retry-interval-s", dbReaderFailedObjectRetryIntervalS);
			request.getSession().setAttribute("src-type", srcTypeStr);
			request.getSession().setAttribute("src-connection-string", srcConnectionString);
			
			if (srcDatabase != null) {
				request.getSession().setAttribute("src-database", srcDatabase);
			} else {
				request.getSession().removeAttribute("src-database");
			}
			
			if (srcSchema != null) {
				request.getSession().setAttribute("src-schema", srcSchema);
			} else {
				request.getSession().removeAttribute("src-schema");
			}
			
			if (srcUser != null) {
				request.getSession().setAttribute("src-user", srcUser);
			} else {
				request.getSession().removeAttribute("src-user");
			}
			
			if (srcDBLink != null) {
				request.getSession().setAttribute("src-dblink", srcDBLink);
			} else {
				request.getSession().removeAttribute("src-dblink");
			}

			if (srcPassword != null) {
				request.getSession().setAttribute("src-password", srcPassword);
			} else {
				request.getSession().removeAttribute("src-password");
			}

			if (srcConnectionInitializationStatement != null) {
				request.getSession().setAttribute("src-connection-initialization-stmt", srcConnectionInitializationStatement);
			} else {
				request.getSession().removeAttribute("src-connection-initialization-stmt");
			}

			request.getSession().setAttribute("src-connection-timeout-s", srcConnectionTimeoutSStr);
			request.getSession().setAttribute("src-object-type", srcObjectType);
			request.getSession().setAttribute("src-object-name-pattern", srcObjectNamePattern);
			request.getSession().setAttribute("src-object-metadata-read-method", srcObjectMetadataReadMethod);
			request.getSession().setAttribute("src-column-metadata-read-method", srcColumnMetadataReadMethod);
			request.getSession().setAttribute("src-constraint-metadata-read-method", srcConstraintMetadataReadMethod);

			request.getSession().setAttribute("src-dbreader-interval-s", srcDBReaderIntervalStr);
			request.getSession().setAttribute("dbreader-stop-after-first-iteration", dbReaderStopAfterFirstIteration);
			request.getSession().setAttribute("src-dbreader-batch-size", srcDBReaderBatchSizeStr);
			request.getSession().setAttribute("src-dbreader-processors", srcDBReaderProcessorsStr);
			request.getSession().setAttribute("src-dbreader-method", srcDBReaderMethodStr);

			request.getSession().setAttribute("src-infer-schema-changes", srcInferSchemaChangesStr);
			request.getSession().setAttribute("src-infer-object-drop", srcInferObjectDropStr);
			request.getSession().setAttribute("src-infer-object-create", srcInferObjectCreateStr);

			request.getSession().setAttribute("src-reload-object-schemas", srcReloadObjectSchemasStr);
			request.getSession().setAttribute("src-reload-object-schemas-on-each-job-restart", srcReloadObjectsSchemasOnEachJobRestartStr);

			request.getSession().setAttribute("src-reload-objects", srcReloadObjectsStr);
			request.getSession().setAttribute("src-reload-objects-on-each-job-restart", srcReloadObjectsOnEachJobRestartStr);

			request.getSession().setAttribute("src-read-null-incremental-key-records", srcReadNullIncrementalKeyRecordsStr);			
			request.getSession().setAttribute("src-compute-max-incremental-key-in-db", srcComputeMaxIncrementalKeyInDBStr);
			request.getSession().setAttribute("src-quote-object-names", srcQuoteObjectNamesStr);
			request.getSession().setAttribute("src-quote-column-names", srcQuoteColumnNamesStr);
			request.getSession().setAttribute("src-use-catalog-scope-resolution", srcUseCatalogScopeResolution);
			request.getSession().setAttribute("src-use-schema-scope-resolution", srcUseSchemaScopeResolution);

			request.getSession().setAttribute("src-numeric-value-mask", srcNumericValueMask);
			request.getSession().setAttribute("src-alphabetic-value-mask", srcAlphabeticValueMask);
			request.getSession().setAttribute("src-dbreader-object-record-limit", srcDBReaderObjectRecordLimitStr);
			if (jvmArguments != null) {
				request.getSession().setAttribute("jvm-arguments", jvmArguments);
			} else {
				request.getSession().removeAttribute("jvm-arguments");
			}
			if ((srcDefaultUniqueKeyColumnList != null) && (!srcDefaultUniqueKeyColumnList.isBlank())) {
				request.getSession().setAttribute("src-default-unique-key-column-list", srcDefaultUniqueKeyColumnList);
			} else {
				request.getSession().removeAttribute("src-default-unique-key-column-list");
			}

			if ((srcDefaultIncrementalKeyColumnList != null) && (!srcDefaultIncrementalKeyColumnList.isBlank())) {
				request.getSession().setAttribute("src-default-incremental-key-column-list", srcDefaultIncrementalKeyColumnList);
			} else {
				request.getSession().removeAttribute("src-default-incremental-key-column-list");
			}

			if ((srcDefaultSoftDeleteCondition != null) && (!srcDefaultSoftDeleteCondition.isBlank())) {
				request.getSession().setAttribute("src-default-soft-delete-condition", srcDefaultSoftDeleteCondition);
			} else {
				request.getSession().removeAttribute("src-default-soft-delete-condition");
			}

			if ((srcDefaultMaskColumnList != null) && (!srcDefaultMaskColumnList.isBlank())) {
				request.getSession().setAttribute("src-default-mask-column-list", srcDefaultMaskColumnList);
			} else {
				request.getSession().removeAttribute("src-default-mask-column-list");
			}

			if ((srcQureyTimestampConversionFunction != null) && (!srcQureyTimestampConversionFunction.isBlank())) {
				request.getSession().setAttribute("src-query-timestamp-conversion-function", srcQureyTimestampConversionFunction);
			} else {
				request.getSession().removeAttribute("src-query-timestamp-conversion-function");
			}

			if ((srcTimestampIncrementalKeyInitialValue != null) && (!srcTimestampIncrementalKeyInitialValue.isBlank())) {
				request.getSession().setAttribute("src-timestamp-incremental-key-initial-value", srcTimestampIncrementalKeyInitialValue);
			} else {
				request.getSession().removeAttribute("src-timestamp-incremental-key-initial-value");
			}

			if ((srcNumericIncrementalKeyInitialValue != null) && (!srcNumericIncrementalKeyInitialValue.isBlank())) {
				request.getSession().setAttribute("src-numeric-incremental-key-initial-value", srcNumericIncrementalKeyInitialValue);
			} else {
				request.getSession().removeAttribute("src-numeric-incremental-key-initial-value");
			}

			//Next step is to fetch all object metadata

			Path dbReaderMetadataFilePath = Path.of(syncLiteDeviceDirStr, "synclite_dbreader_metadata.db");
			createMetadataTables(dbReaderMetadataFilePath);

			SrcInfo srcInfo = new SrcInfo();
			srcInfo.type = srcType;
			srcInfo.connectionString = srcConnectionString;
			srcInfo.connectionTimeoutMs = srcConnectionTimeoutMs;
			srcInfo.user = srcUser;
			srcInfo.password = srcPassword;
			srcInfo.catalog = srcDatabase;
			srcInfo.schema = srcSchema;
			srcInfo.dbLink = srcDBLink;
			srcInfo.objectType = srcObjectType;
			srcInfo.objectNamePattern = srcObjectNamePattern;
			srcInfo.csvFormat = csvFormat;
			srcInfo.hasColumnMetadata = hasColumnMetadata;

			int numObjects = loadObjectFromMetadata(request, dbReaderMetadataFilePath, srcInfo, reloadObjectSchemas, srcObjectMetadataReadMethod, srcColumnMetadataReadMethod, srcConstraintMetadataReadMethod);
			request.getSession().setAttribute("num-enabled-objects", numObjects);
			response.sendRedirect("configureTableOptions.jsp");

		} catch (Exception e) {
			//System.out.println("exception : " + e);
			this.globalTracer.error("Exception while processing request:", e);
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("configureDBReader.jsp?errorMsg=" + errorMsg).forward(request, response);
			throw new ServletException(e);
		}
	}

	private void validateMongoDBConnection(String srcConnectionString, String dbName, boolean validateLogBasedReplication) throws ServletException {
		try (MongoClient mongoClient = MongoClients.create(srcConnectionString)) {
			MongoDatabase db = mongoClient.getDatabase(dbName);
			db.listCollectionNames().first();
			if (validateLogBasedReplication) {
				//Disable for now.
				/*
				try {
		            Document isMasterResult = mongoClient.getDatabase("admin").runCommand(new Document("isMaster", 1));
		            if (! isMasterResult.containsKey("setName")) {
		            	throw new ServletException("LOG_BASED DBReader method for MongoDB requires replica sets configured on the source MongoDB. Specified MongoDB is not part of a replica set");
		            }
				} catch (Exception e) {
					throw new ServletException("Failed to validate replica set configuration for specified MongoDB source : " + e.getMessage(), e);
				}
				*/
			}
		} catch (Exception e) {
			throw new ServletException("Failed to connect to source MongoDB database : " + e.getMessage(), e);
		}
	}

	private void validatePath(Path dir) throws ServletException {
		if ((dir == null) || dir.toString().isBlank()) {
			this.globalTracer.error("Please verify a valid directory path.");
			throw new ServletException("Please verify a valid directory path.");			
		}
		if (Files.exists(dir)) {
			if (!dir.toFile().isDirectory()) {
				this.globalTracer.error("The specified path is not a directory : " + dir + ".");
				throw new ServletException("The specified path is not a directory : " + dir + ".");
			}
			if (!dir.toFile().canRead()) {
				this.globalTracer.error("The specified directory is not readable : " + dir + ".");
				throw new ServletException("The specified directory is not readable : " + dir + ".");		
			}
		} else {
			this.globalTracer.error("The specified directory does not exist : " + dir + ".");
			throw new ServletException("The specified directory does not exist : " + dir + ".");		
		}	

	}

	private HashMap<String, String> loadObjectNamesFromMetadata(HttpServletRequest request, Connection conn, SrcInfo srcInfo, String srcObjectMetadataReadMethod) throws SQLException {
		if (srcObjectMetadataReadMethod.equals("JDBC")) {
			//this.globalTracer.info("Using JDBC method for reading object metadata");
			return loadObjectNamesFromMetadataDefault(conn, srcInfo);
		} else {
			//this.globalTracer.info("Using NATIVE method for reading object metadata");
			switch (srcInfo.type) {
			case CSV:
				return loadObjectNamesFromMetadataCSV(request, conn, srcInfo);
			case MONGODB:
				return loadObjectNamesFromMetadataMongoDB(request, conn, srcInfo);
			case MYSQL:
				return loadObjectNamesFromMetadataMySQL(conn, srcInfo);
			case POSTGRESQL:
				return loadObjectNamesFromMetadataPG(conn, srcInfo);
			default:
				return loadObjectNamesFromMetadataDefault(conn, srcInfo);
			}
		}
	}

	private HashMap<String, String> loadObjectNamesFromMetadataMongoDB(HttpServletRequest request, Connection conn, SrcInfo srcInfo) throws SQLException {
		HashMap<String, String> objectMap= new HashMap<String, String>();
		try {
			try (MongoClient mongoClient = MongoClients.create(srcInfo.connectionString)) {
				MongoDatabase db = mongoClient.getDatabase(srcInfo.catalog);
				for (String collectionName : db.listCollectionNames()) {
					objectMap.put(collectionName, "TABLE");
				}
			}
		} catch (Exception e) {
			throw new SQLException("Failed to get collection names from source MongoDB database : " + e.getMessage(), e);
		}
		return objectMap;
	}

	
	private HashMap<String, String> loadObjectNamesFromMetadataCSV(HttpServletRequest request, Connection conn, SrcInfo srcInfo) throws SQLException {
		//Get all folder names in the given path.
		HashMap<String, String> objectMap= new HashMap<String, String>();

		downloadObjectsInLocalFSDir(request);
		Path dataDirPath = Path.of(request.getSession().getAttribute("src-file-storage-local-fs-directory").toString());

		try (Connection c = DriverManager.getConnection("jdbc:sqlite::memory:"); 
				Statement s = c.createStatement();
				DirectoryStream<Path> stream = Files.newDirectoryStream(dataDirPath)) {
			// Iterate over the directory contents
			for (Path entry : stream) {
				// Check if the entry is a directory
				if (Files.isDirectory(entry)) {
					//Check objectName pattern
					String objName = entry.getFileName().toString();
					try (ResultSet rs = s.executeQuery("SELECT 1 WHERE '" + objName + "' LIKE '" + srcInfo.objectNamePattern + "'")) {
						if (rs.next()) {
							objectMap.put(objName, "TABLE");
						}
					}
				}
			}
		} catch (IOException e) {
			throw new SQLException("Failed to read contents of specified data directory path : " + srcInfo.connectionString + " : " + e.getMessage(), e);
		}
		return objectMap;
	}

	private void downloadObjectsInLocalFSDir(HttpServletRequest request) throws SQLException {

		String storageType = request.getSession().getAttribute("src-file-storage-type").toString();

		switch(storageType) {
		case "SFTP":

			//List all directories under specified remote directory path.
			downloadObjectsFromSFTPToLocalFSDir(request);
			break;
		case "S3":

			//List all directories under specified remote directory path.
			downloadObjectsFromS3ToLocalFSDir(request);
			break;

		}		
	}

	private final void downloadObjectsFromSFTPToLocalFSDir(HttpServletRequest request) throws SQLException {
		try {			
			String user = request.getSession().getAttribute("src-file-storage-sftp-user").toString();
			String password = request.getSession().getAttribute("src-file-storage-sftp-password").toString();
			String host = request.getSession().getAttribute("src-file-storage-sftp-host").toString();
			Integer port = Integer.valueOf(request.getSession().getAttribute("src-file-storage-sftp-port").toString());
			String remoteDirectory = request.getSession().getAttribute("src-file-storage-sftp-directory").toString();

			java.util.Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");
			JSch jsch = new JSch();
			//jsch.addIdentity("/home/ubuntu/.ssh/id_rsa");
			Session session = jsch.getSession(user, host, port);
			//session.setConfig("PreferredAuthentications", "publickey, keyboard-interactive,password");
			session.setPassword(password);            
			session.setConfig(config);
			session.connect();
			ChannelSftp channel = (ChannelSftp) session.openChannel("sftp");
			channel.connect();

			Path localDirPath = Path.of(request.getSession().getAttribute("src-file-storage-local-fs-directory").toString());
			//List objects now.

			channel.cd(remoteDirectory);
			Vector<ChannelSftp.LsEntry> entries = channel.ls(remoteDirectory);

			for (ChannelSftp.LsEntry entry : entries) {
				String objName = entry.getFilename();				
				//Now create a directory with this name under localFS directory
				Path localObjPath = localDirPath.resolve(objName);
				Files.createDirectories(localObjPath);

				//Now check if at least one file exists under the path then download it as well

				String remoteObjPath = remoteDirectory + "/" + objName;

				channel.cd(remoteObjPath);
				Vector<ChannelSftp.LsEntry> fileEntries = channel.ls(remoteObjPath);

				for (ChannelSftp.LsEntry fileEntry : fileEntries) {
					String fileName = fileEntry.getFilename();

					String remoteFilePath = remoteDirectory + "/" + objName + "/" + fileName;
					String localFilePath = localDirPath.resolve(objName).resolve(fileName).toString();

					// Download the file
					try (OutputStream outputStream = new FileOutputStream(localFilePath)) {
						channel.get(remoteFilePath.toString(), outputStream);					
					}
					break;
				}
			}			
			channel.disconnect();
			session.disconnect();
		} catch (Exception e) {
			throw new SQLException("Failed to list objects from the specified SFTP location : " + e.getMessage(), e);
		}
	}

	private final void downloadObjectsFromS3ToLocalFSDir(HttpServletRequest request) throws SQLException {
		try {			
			String url = request.getSession().getAttribute("src-file-storage-s3-url").toString();
			String bucketName = request.getSession().getAttribute("src-file-storage-s3-bucket-name").toString();
			String accessKey = request.getSession().getAttribute("src-file-storage-s3-access-key").toString();
			String secretKey = request.getSession().getAttribute("src-file-storage-s3-secret-key").toString();

			AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

			AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(credentials))
					.withEndpointConfiguration(new EndpointConfiguration(url, AwsHostNameUtils.parseRegion(url, null)))
					.build();


			Path localDirPath = Path.of(request.getSession().getAttribute("src-file-storage-local-fs-directory").toString());

			ListObjectsV2Result result = s3Client.listObjectsV2(bucketName);				
			for (S3ObjectSummary ob : result.getObjectSummaries()) {
				String key = ob.getKey(); 
				String[] keyParts = key.split("/");
				String objName = "";
				if (key.endsWith("/")) {
					objName = keyParts[keyParts.length - 2];
					Path localObjPath = localDirPath.resolve(objName);
					Files.createDirectories(localObjPath);

					//Now check if at least one file exists under the path then download it as well
					ListObjectsV2Result fileResults = s3Client.listObjectsV2(bucketName + "/" + key);		             
					for (S3ObjectSummary fileOb : fileResults.getObjectSummaries()) {
						String fileName = key.split("/")[keyParts.length - 1];

						String remoteFilePath = bucketName + "/" + objName + "/" + fileName;
						String localFilePath = localDirPath.resolve(objName).resolve(fileName).toString();

						// Download the file
						S3Object s3Object = s3Client.getObject(bucketName, remoteFilePath.toString());	            
						try (S3ObjectInputStream inputStream = s3Object.getObjectContent()) {
							try (FileOutputStream outputStream = new FileOutputStream(Path.of(localFilePath).toFile())) {
								int read = 0;
								byte[] bytes = new byte[2048];
								while ((read = inputStream.read(bytes)) != -1) {
									outputStream.write(bytes, 0, read);
								}
								outputStream.flush();
							}
						}
						break;
					}		        
				}   
			}
			s3Client.shutdown();
		} catch (Exception e) {
			throw new SQLException("Failed to list objects from the specified S3 location : " + e.getMessage(), e);
		}
	}

	private HashMap <String, String> loadObjectNamesFromMetadataDefault(Connection conn, SrcInfo srcInfo) throws SQLException {
		// Get a list of all table names in the database
		String[] objTypes = null;
		DatabaseMetaData metaData = conn.getMetaData();
		HashMap<String, String> objectMap= new HashMap<String, String>();
		switch(srcInfo.objectType) {
		case "TABLE":
			objTypes = new String[] {"TABLE"};
			try (ResultSet tables = metaData.getTables(srcInfo.catalog, srcInfo.schema, srcInfo.objectNamePattern, objTypes)) {
				while (tables.next()) {
					String tableName = tables.getString("TABLE_NAME");
					objectMap.put(tableName, "TABLE");
				}
			}
			break;
		case "VIEW":
			objTypes = new String[] {"VIEW"};
			try (ResultSet tables = metaData.getTables(srcInfo.catalog, srcInfo.schema, srcInfo.objectNamePattern, objTypes)) {
				while (tables.next()) {
					String tableName = tables.getString("TABLE_NAME");
					objectMap.put(tableName, "VIEW");
				}
			}
			break;
		case "ALL" :
			objTypes = new String[] {"TABLE"};
			try (ResultSet tables = metaData.getTables(srcInfo.catalog, srcInfo.schema, srcInfo.objectNamePattern, objTypes)) {
				while (tables.next()) {
					String tableName = tables.getString("TABLE_NAME");
					objectMap.put(tableName, "TABLE");
				}
			}
			objTypes = new String[] {"VIEW"};
			try (ResultSet tables = metaData.getTables(srcInfo.catalog, srcInfo.schema, srcInfo.objectNamePattern, objTypes)) {
				while (tables.next()) {
					String tableName = tables.getString("TABLE_NAME");
					objectMap.put(tableName, "VIEW");
				}
			}
		}

		return objectMap;
	}

	private HashMap<String, String> loadObjectNamesFromMetadataPG(Connection conn, SrcInfo srcInfo) throws SQLException {
		// Get a list of all object names in the database
		HashMap<String, String> objectMap= new HashMap<String, String>();

		String sql = "";
		switch(srcInfo.objectType) {		
		case "TABLE":
			sql = "SELECT TABLE_NAME, 'TABLE' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '" + srcInfo.catalog + "' AND TABLE_SCHEMA = '" + srcInfo.schema + "' AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME LIKE '" + srcInfo.objectNamePattern + "'";
			break;
		case "VIEW":
			sql = "SELECT TABLE_NAME, 'VIEW' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '" + srcInfo.catalog + "' AND TABLE_SCHEMA = '" + srcInfo.schema + "' AND TABLE_TYPE = 'VIEW' AND TABLE_NAME LIKE '" + srcInfo.objectNamePattern + "'";
			break;
		case "ALL":
			String tableSql = "SELECT TABLE_NAME, 'TABLE' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '" + srcInfo.catalog + "' AND TABLE_SCHEMA = '" + srcInfo.schema + "' AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME LIKE '" + srcInfo.objectNamePattern + "'";
			String viewSql = "SELECT TABLE_NAME, 'VIEW' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '" + srcInfo.catalog + "' AND TABLE_SCHEMA = '" + srcInfo.schema + "' AND TABLE_TYPE = 'VIEW' AND TABLE_NAME LIKE '" + srcInfo.objectNamePattern + "'";
			sql = tableSql + " UNION " + viewSql;
			break;
		}

		try (Statement stmt = conn.createStatement()) {
			try (ResultSet rs = stmt.executeQuery(sql)) {
				while(rs.next()) {
					String tabName = rs.getString(1);
					String tabType = rs.getString(2);
					objectMap.put(tabName, tabType);
				}
			}
		}
		return objectMap;
	}

	private HashMap<String, String> loadObjectNamesFromMetadataMySQL(Connection conn, SrcInfo srcInfo) throws SQLException {
		// Get a list of all object names in the database
		HashMap<String, String> objectMap= new HashMap<String, String>();

		String sql = "";
		switch(srcInfo.objectType) {		
		case "TABLE":
			sql = "SELECT TABLE_NAME, 'TABLE' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + srcInfo.schema + "' AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME LIKE '" + srcInfo.objectNamePattern + "'";
			break;
		case "VIEW":
			sql = "SELECT TABLE_NAME, 'VIEW' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + srcInfo.schema + "' AND TABLE_TYPE = 'VIEW' AND TABLE_NAME LIKE '" + srcInfo.objectNamePattern + "'";
			break;
		case "ALL":
			String tableSql = "SELECT TABLE_NAME, 'TABLE' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + srcInfo.schema + "' AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME LIKE '" + srcInfo.objectNamePattern + "'";
			String viewSql = "SELECT TABLE_NAME, 'VIEW' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + srcInfo.schema + "' AND TABLE_TYPE = 'VIEW' AND TABLE_NAME LIKE '" + srcInfo.objectNamePattern + "'";
			sql = tableSql + " UNION " + viewSql;
			break;
		}

		try (Statement stmt = conn.createStatement()) {
			try (ResultSet rs = stmt.executeQuery(sql)) {
				while(rs.next()) {
					String tabName = rs.getString(1);
					String tabType = rs.getString(2);
					objectMap.put(tabName, tabType);
				}
			}
		}
		return objectMap;
	}

	private String loadPKColsFromMetadata(HttpServletRequest request, Connection conn, SrcInfo srcInfo, String objectName, String srcConstraintMetadataReadMethod) throws SQLException {
		if (srcConstraintMetadataReadMethod.equals("JDBC")) {
			//this.globalTracer.info("Using JDBC method for reading constraint metadata for object : " + objectName);
			return loadPKColsFromMetadataDefault(conn, srcInfo, objectName);
		} else {
			//this.globalTracer.info("Using NATIVE method for reading constraint metadata for object : " + objectName);
			switch (srcInfo.type) {
			case CSV:
				return loadPKColsFromMetadataCSV(conn, srcInfo, objectName);
			case MONGODB:
				return loadPKColsFromMetadataMongoDB(conn, srcInfo, objectName);
			case POSTGRESQL:
				return loadPKColsFromMetadataPG(conn, srcInfo, objectName);
			default:
				return loadPKColsFromMetadataDefault(conn, srcInfo, objectName);
			}		
		}
	}

	private String loadPKColsFromMetadataMongoDB(Connection conn, SrcInfo srcInfo, String objectName) {
		return "_id";
	}

	private String loadPKColsFromMetadataCSV(Connection conn, SrcInfo srcInfo, String objectName) {
		//Return an empty string from here
		return "";
	}

	private String loadPKColsFromMetadataDefault(Connection conn, SrcInfo srcInfo, String objectName) throws SQLException {
		StringBuilder pkColListBuilder = new StringBuilder();
		HashSet<String> processedPKCols = new HashSet<String>();
		DatabaseMetaData metaData = conn.getMetaData();
		try (ResultSet primaryKeys = metaData.getPrimaryKeys(srcInfo.catalog, srcInfo.schema, objectName)) {
			boolean first = true;
			while (primaryKeys.next()) {
				String primaryKeyColumnName = primaryKeys.getString("COLUMN_NAME");
				if (primaryKeyColumnName == null) {
					continue;
				}							
				if (processedPKCols.contains(primaryKeyColumnName)) {
					continue;
				}
				if (!first) {
					pkColListBuilder.append(",");
				}
				pkColListBuilder.append(primaryKeyColumnName);
				first = false;
				processedPKCols.add(primaryKeyColumnName);
			}
		}
		// If no primary key is found, get any one unique key
		if (pkColListBuilder.length() == 0) {
			processedPKCols.clear();
			try (ResultSet uniqueKeys = metaData.getIndexInfo(srcInfo.catalog, srcInfo.schema, objectName, true, false)) {
				boolean first = true;
				while (uniqueKeys.next()) {
					String columnName = uniqueKeys.getString("COLUMN_NAME");
					// Ensure the column is part of the unique key
					if (columnName == null) {
						continue;
					}							
					if (processedPKCols.contains(columnName)) {
						continue;
					}

					if (!first) {
						pkColListBuilder.append(",");
					}
					pkColListBuilder.append(columnName);
					first = false;
					processedPKCols.add(columnName);
				}
			}
		}
		return pkColListBuilder.toString();
	}

	private String loadPKColsFromMetadataInternal(Connection conn, String catalog, String schema, String objectName, String pkConsColsSql, String ukConsSql, String ukConsColsSql) throws SQLException {
		int pkCols = 0;
		StringBuilder pkColListBuilder = new StringBuilder();
		try (PreparedStatement pkConsPStmt = conn.prepareStatement(pkConsColsSql)) {
			if (catalog == null) {
				pkConsPStmt.setString(1, schema);
				pkConsPStmt.setString(2, objectName);
			} else {
				pkConsPStmt.setString(1, catalog);
				pkConsPStmt.setString(2, schema);
				pkConsPStmt.setString(3, objectName);			
			}

			try (ResultSet rsPKCons = pkConsPStmt.executeQuery()) {
				while (rsPKCons.next()) {
					//We found PK
					String colName = rsPKCons.getString(1);
					if (pkCols > 0) {
						pkColListBuilder.append(",");						
					}
					pkColListBuilder.append(colName);
					++pkCols;
				}
			}
		}
		if (pkCols == 0) {
			//We did not find a PK. find the first UK
			try (PreparedStatement ukConsPStmt = conn.prepareStatement(ukConsSql)) {
				if (catalog == null) {
					ukConsPStmt.setString(1, schema);
					ukConsPStmt.setString(2, objectName);
				} else {
					ukConsPStmt.setString(1, catalog);
					ukConsPStmt.setString(2, schema);
					ukConsPStmt.setString(3, objectName);
				}
				try (ResultSet rsUKCons = ukConsPStmt.executeQuery()) {
					if (rsUKCons.next()) {
						//UK found
						//Get col names in this UK
						String ukName = rsUKCons.getString(1);
						try (PreparedStatement ukConsColsPStmt = conn.prepareStatement(ukConsColsSql)) {
							if (catalog == null) {
								ukConsColsPStmt.setString(1, schema);
								ukConsColsPStmt.setString(2, objectName);
								ukConsColsPStmt.setString(3, ukName);
							} else {
								ukConsColsPStmt.setString(1, catalog);
								ukConsColsPStmt.setString(2, schema);
								ukConsColsPStmt.setString(3, objectName);
								ukConsColsPStmt.setString(4, ukName);
							}
							try (ResultSet rsUKConsCols = ukConsColsPStmt.executeQuery()) {
								int ukCols = 0;
								while (rsUKConsCols.next()) {
									//We found PK
									String colName = rsUKConsCols.getString(1);
									if (ukCols > 0) {
										pkColListBuilder.append(",");						
									}
									pkColListBuilder.append(colName);
									++ukCols;
								}							
							}
						}
					}
				}			
			}
		}		
		return pkColListBuilder.toString();
	}

	private String loadPKColsFromMetadataPG(Connection conn, SrcInfo srcInfo, String objectName) throws SQLException {

		String pkConsColsSql = "SELECT kcu.column_name FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_catalog = tc.constraint_catalog AND kcu.constraint_schema = tc.constraint_schema AND kcu.constraint_name = tc.constraint_name WHERE kcu.table_catalog = ? AND kcu.table_schema = ? AND kcu.table_name = ? AND tc.constraint_type = 'PRIMARY KEY' ORDER BY kcu.ordinal_position";

		String ukConsSql = "SELECT constraint_name FROM information_schema.table_constraints WHERE table_catalog = ? AND table_schema = ? AND table_name = ? AND constraint_type = 'UNIQUE' ORDER BY constraint_name";

		String ukConsColsSql = "SELECT kcu.column_name	FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_catalog = tc.constraint_catalog AND kcu.constraint_schema = tc.constraint_schema AND kcu.constraint_name = tc.constraint_name WHERE kcu.table_catalog = ? AND kcu.table_schema = ? AND kcu.table_name = ? AND tc.constraint_name = ? ORDER BY kcu.ordinal_position";

		return loadPKColsFromMetadataInternal(conn, srcInfo.catalog, srcInfo.schema, objectName, pkConsColsSql, ukConsSql, ukConsColsSql);

	}

	private String loadPKColsFromMetadataMySQL(Connection conn, SrcInfo srcInfo, String objectName) throws SQLException {

		String pkConsColsSql = "SELECT column_name FROM information_schema.key_column_usage WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY' ORDER BY ordinal_position";

		String ukConsSql = "SELECT constraint_name FROM information_schema.table_constraints WHERE table_schema = ? AND table_name = ? AND constraint_type = 'UNIQUE' ORDER BY constraint_name";

		String ukConsColsSql = "SELECT column_name FROM information_schema.key_column_usage WHERE table_schema = ? AND table_name = ? AND constraint_name = ? ORDER BY ordinal_position";

		return loadPKColsFromMetadataInternal(conn, null, srcInfo.schema, objectName, pkConsColsSql, ukConsSql, ukConsColsSql);
	}

	//MAKE SURE THAT THE IMPLEMENTATION HERE IS EXACTLY IDENTICAL TO THAT IN DBReader
	private int loadObjectFromMetadata(HttpServletRequest request, Path dbReaderMetadataFilePath, SrcInfo srcInfo, boolean reloadObjectSchemas, String srcObjectMetadataReadMethod, String srcColumnMetadataReadMethod, String srcConstraintMetadataReadMethod) throws ServletException {
		this.globalTracer.info("Loading objects from source DB metadata using methods : ");
		this.globalTracer.info("Object metadata : " + srcObjectMetadataReadMethod);
		this.globalTracer.info("Column metadata : " + srcColumnMetadataReadMethod);
		this.globalTracer.info("Constraint metadata : " + srcConstraintMetadataReadMethod);

		if ((srcInfo.catalog == null) && (srcInfo.schema != null)) {
			srcInfo.catalog = srcInfo.schema;			
		}
		if ((srcInfo.catalog != null) && (srcInfo.schema == null)) {
			srcInfo.schema = srcInfo.catalog;
		}

		//Make sure the logic below is in sync with the one in DBreader
		//this.globalTracer.error("Fetching objects for : " + srcDatabase + " : " + srcSchema);
		Properties properties = new Properties();
		if (srcInfo.user != null) {
			properties.setProperty("user", srcInfo.user);
		}
		if (srcInfo.password != null) {
			properties.setProperty("password", srcInfo.password);
		}
		properties.setProperty("connectionTimeout", String.valueOf(srcInfo.connectionTimeoutMs));
		try (Connection connection = getSrcConnection(srcInfo, properties)) {
			HashMap<String, String> objects = loadObjectNamesFromMetadata(request, connection, srcInfo, srcObjectMetadataReadMethod);

			// Get a list of all object names in the database
			List<ObjectInfo> tiList =  new ArrayList<ObjectInfo>();
			for (Map.Entry<String, String> objectEntry: objects.entrySet()) {
				//Get a list of column names and data types for the current object
				String columnList = loadColumnsFromMetadata(request, connection, srcInfo, objectEntry.getKey(), srcColumnMetadataReadMethod);
				String pkColList = loadPKColsFromMetadata(request, connection, srcInfo, objectEntry.getKey(), srcConstraintMetadataReadMethod);
				ObjectInfo ti = new ObjectInfo();
				ti.objectName = objectEntry.getKey();
				ti.objectType = objectEntry.getValue();
				ti.columnList = columnList;
				ti.pkColList = pkColList;
				ti.reloadObjectSchemas = reloadObjectSchemas;

				tiList.add(ti);
			}
			if (tiList.size() > 0) {
				addSrcObjectInfoToMetadataTable(dbReaderMetadataFilePath, tiList);
			}
			this.globalTracer.info("Finished loading " + tiList.size() +" objects from source DB metadata");
			return tiList.size();

		} catch (Exception e) {
			this.globalTracer.error("Failed to read object metadata from source database : " + e.getMessage(), e);
			throw new ServletException("Failed to read object metadata from source database : " + e.getMessage(), e);
		}
	}

	private Connection getSrcConnection(SrcInfo srcInfo, Properties properties) throws SQLException {
		switch (srcInfo.type) {
		case CSV:
			return null;
		case MONGODB:
			return null;
		default :		
			return DriverManager.getConnection(srcInfo.connectionString, properties);
		}
	}

	//MAKE SURE THAT THE IMPLEMENTATION HERE IS EXACTLY IDENTICAL TO THAT IN DBReader
	private String loadColumnsFromMetadataPG(Connection conn, SrcInfo srcInfo, String objectName) throws SQLException {
		JSONArray columnListBuilder = new JSONArray();
		HashSet<String> processedColumns = new HashSet<String>();

		try (
				PreparedStatement stmt = conn.prepareStatement(
						"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE, UDT_NAME " +
								"FROM INFORMATION_SCHEMA.COLUMNS " +
								"WHERE TABLE_CATALOG = ? AND TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
						);
				) {

			stmt.setString(1, srcInfo.catalog);
			stmt.setString(2, srcInfo.schema);
			stmt.setString(3, objectName);
			ResultSet columnsInfo = stmt.executeQuery();

			while (columnsInfo.next()) {
				String columnName = columnsInfo.getString("COLUMN_NAME");
				if (processedColumns.contains(columnName)) {
					continue;
				}

				// Get data type and character maximum length
				String columnTypeName = columnsInfo.getString("DATA_TYPE");
				int characterMaxLength = columnsInfo.getInt("CHARACTER_MAXIMUM_LENGTH");
				String isNullable = getIsNullableClause(columnsInfo.getString("IS_NULLABLE"));
				int dataPrecision = columnsInfo.getInt("NUMERIC_PRECISION");
				int dataScale = columnsInfo.getInt("NUMERIC_SCALE");
				String udtName = columnsInfo.getString("UDT_NAME");

				//Handle Array data type
				if (columnTypeName.equalsIgnoreCase("ARRAY")) {
					//
					switch(udtName.toLowerCase()) {
					case "_int4":
						columnTypeName = "INTEGER[]";
						break;
					case "_int8":
						columnTypeName = "BIGINT[]";
						break;
					case "_boolean":
						columnTypeName = "BOOLEAN[]";
						break;
					case "_float8":
						columnTypeName = "FLOAT[]";
						break;
					case "_numeric":
						columnTypeName = "NUMERIC[]";
						break;
					case "_timestamp":
						columnTypeName = "TIMESTAMP[]";
						break;
					case "_date":
						columnTypeName = "DATE[]";
						break;
					case "_time":
						columnTypeName = "TIME[]";
						break;
					case "_bpchar":
						columnTypeName = "CHARACTER[]";
						break;
					case "_text":
					default:
						columnTypeName = "TEXT[]";
						break;
					}
				}
				// Append column name, data type, and maximum character length for applicable types
				StringBuilder columnBuilder = new StringBuilder();
				if (columnName.matches(".*\\s+.*")) {
					//column name contains whitestspaces , quote it
					columnBuilder.append("\"");
					columnBuilder.append(columnName);
					columnBuilder.append("\"");
				} else {
					columnBuilder.append(columnName);
				}
				columnBuilder.append(" ");
				columnBuilder.append(columnTypeName.toUpperCase());

				if (!columnTypeName.toUpperCase().equals("TEXT") && (characterMaxLength > 0)) {
					columnBuilder.append("(");
					columnBuilder.append(characterMaxLength);
					columnBuilder.append(")");
				} else if (isPGNumericWithPrecisionScaleDataType(columnTypeName)) {
					if (dataPrecision > 0) {
						columnBuilder.append("(");
						columnBuilder.append(dataPrecision);
						if (dataScale > 0) {
							columnBuilder.append(",");
							columnBuilder.append(dataScale);
						}
						columnBuilder.append(")");
					}
				} 
				// Append NULL/NOT NULL
				columnBuilder.append(" ");
				columnBuilder.append(isNullable.toUpperCase());

				columnListBuilder.put(columnBuilder.toString());
				processedColumns.add(columnName);
			}
		}
		return columnListBuilder.toString(1);
	}

	//MAKE SURE THAT THE IMPLEMENTATION HERE IS EXACTLY IDENTICAL TO THAT IN DBReader
	private String loadColumnsFromMetadataMySQL(Connection conn, SrcInfo srcInfo, String objectName) throws SQLException {
		JSONArray columnListBuilder = new JSONArray();
		HashSet<String> processedColumns = new HashSet<String>();

		try (
				PreparedStatement stmt = conn.prepareStatement(
						"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE " +
								"FROM INFORMATION_SCHEMA.COLUMNS " +
								"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
						);
				) {

			stmt.setString(1, srcInfo.schema);
			stmt.setString(2, objectName);
			ResultSet columnsInfo = stmt.executeQuery();

			while (columnsInfo.next()) {
				String columnName = columnsInfo.getString("COLUMN_NAME");
				if (processedColumns.contains(columnName)) {
					continue;
				}

				// Get data type and character maximum length
				String columnTypeName = columnsInfo.getString("DATA_TYPE");
				int characterMaxLength = columnsInfo.getInt("CHARACTER_MAXIMUM_LENGTH");
				String isNullable = getIsNullableClause(columnsInfo.getString("IS_NULLABLE"));
				int dataPrecision = columnsInfo.getInt("NUMERIC_PRECISION");
				int dataScale = columnsInfo.getInt("NUMERIC_SCALE");

				// Append column name, data type, and maximum character length for applicable types
				StringBuilder columnBuilder = new StringBuilder();
				if (columnName.matches(".*\\s+.*")) {
					//column name contains whitestspaces , quote it
					columnBuilder.append("\"");
					columnBuilder.append(columnName);
					columnBuilder.append("\"");
				} else {
					columnBuilder.append(columnName);
				}
				columnBuilder.append(" ");
				columnBuilder.append(columnTypeName.toUpperCase());

				if (!columnTypeName.toUpperCase().equals("TEXT") && (characterMaxLength > 0)) {
					columnBuilder.append("(");
					columnBuilder.append(characterMaxLength);
					columnBuilder.append(")");
				} else if (isMySQLNumericWithPrecisionScaleDataType(columnTypeName)) {
					if (dataPrecision > 0) {
						columnBuilder.append("(");
						columnBuilder.append(dataPrecision);
						if (dataScale > 0) {
							columnBuilder.append(",");
							columnBuilder.append(dataScale);
						}
						columnBuilder.append(")");
					}
				}
				// Append NULL/NOT NULL
				columnBuilder.append(" ");
				columnBuilder.append(isNullable.toUpperCase());

				columnListBuilder.put(columnBuilder.toString());
				processedColumns.add(columnName);
			}
		}
		return columnListBuilder.toString(1);
	}

	private boolean isPGNumericWithPrecisionScaleDataType(String dataType) {
		return (dataType.equalsIgnoreCase("NUMERIC") || dataType.equalsIgnoreCase("DECIMAL"));
	}

	private boolean isMySQLNumericWithPrecisionScaleDataType(String dataType) {
		return (dataType.equalsIgnoreCase("NUMERIC") || dataType.equalsIgnoreCase("DECIMAL"));
	}

	private String loadColumnsFromMetadata(HttpServletRequest request, Connection conn, SrcInfo srcInfo, String objectName, String srcColumnMetadataReadMethod) throws SQLException {
		if (srcColumnMetadataReadMethod.equals("JDBC")) {
			//this.globalTracer.info("Using JDBC method for reading column metadata for object : " + objectName);
			return loadColumnsFromMetadataDefault(conn, srcInfo, objectName);
		} else {
			//this.globalTracer.info("Using NATIVE method for reading column metadata for table : " + objectName);
			switch (srcInfo.type) {
			case CSV:
				return loadColumnsFromMetadataCSV(request, conn, srcInfo, objectName);
			case MONGODB:
				return loadColumnsFromMetadataMongoDB(request, conn, srcInfo, objectName);
			case MYSQL:
				srcInfo.catalog = "def";
				return loadColumnsFromMetadataMySQL(conn, srcInfo, objectName);
			case POSTGRESQL:
				return loadColumnsFromMetadataPG(conn, srcInfo, objectName);
			default:
				return loadColumnsFromMetadataDefault(conn, srcInfo, objectName);
			}
		}
	}

	private String loadColumnsFromMetadataMongoDB(HttpServletRequest request, Connection conn, SrcInfo srcInfo, String objectName) {
		JSONArray columnListBuilder = new JSONArray();
		StringBuilder columnBuilder = new StringBuilder();
		columnBuilder.append("_id ObjectId NOT NULL");
		columnListBuilder.put(columnBuilder.toString());

		columnBuilder = new StringBuilder();
		columnBuilder.append("document BSON NOT NULL");
		columnListBuilder.put(columnBuilder.toString());
		return columnListBuilder.toString(1);
	}

	private String loadColumnsFromMetadataCSV(HttpServletRequest request, Connection conn, SrcInfo srcInfo, String objectName) throws SQLException {
		JSONArray columnListBuilder = new JSONArray();

		// Get the Path object for the directory
		Path dir = Path.of(request.getSession().getAttribute("src-file-storage-local-fs-directory").toString(), objectName);

		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
			// Iterate over the directory contents
			for (Path entry : stream) {
				// Check if the entry is a file and ends with ".csv"
				if (Files.isRegularFile(entry)) {
					// Parse the CSV file and read its header
					try (Reader reader = new FileReader(entry.toString());
							CSVParser parser = new CSVParser(reader, srcInfo.csvFormat)) {
						CSVRecord header = parser.iterator().next();
						int colIndex = 1;
						for (String columnName : header) {
							String actualColName = columnName;
							if (!srcInfo.hasColumnMetadata) {
								actualColName = "col" + colIndex;
							}

							StringBuilder columnBuilder = new StringBuilder();
							if (columnName.matches(".*\\s+.*")) {
								//column name contains whitestspaces , quote it
								columnBuilder.append("\"");
								columnBuilder.append(actualColName);
								columnBuilder.append("\"");
							} else {
								columnBuilder.append(actualColName);
							}
							columnBuilder.append(" TEXT");
							columnBuilder.append(" NULL");
							columnListBuilder.put(columnBuilder.toString());
							++colIndex;
						}
					}
				}
				break;
			}
		} catch (IOException e) {
			throw new SQLException("Failed to read contents if the object directory : " + srcInfo.connectionString + " for object : " + objectName + " : " + e.getMessage(), e);
		}

		return columnListBuilder.toString(1);
	}

	private String loadColumnsFromMetadataDefault(Connection conn, SrcInfo srcInfo, String objectName) throws SQLException {
		JSONArray columnListBuilder = new JSONArray();
		HashSet<String> processedColumns = new HashSet<String>();
		DatabaseMetaData metaData = conn.getMetaData();
		try (ResultSet columns = metaData.getColumns(srcInfo.catalog, srcInfo.schema, objectName, null)) {
			while (columns.next()) {
				String columnName = columns.getString("COLUMN_NAME");
				if (processedColumns.contains(columnName)) {
					continue;
				}

				StringBuilder columnBuilder = new StringBuilder();
				String columnTypeName = columns.getString("TYPE_NAME");
				int columnSize = columns.getInt("COLUMN_SIZE");
				int decimalDigits = columns.getInt("DECIMAL_DIGITS");
				//this.globalTracer.info("column : " + objectName + "." + columnName);
				String isNullable = getIsNullableClause(columns.getString("IS_NULLABLE")); // NULL or NOT NULL
				if (columnName.matches(".*\\s+.*")) {
					//column name contains whitestspaces , quote it
					columnBuilder.append("\"");
					columnBuilder.append(columnName);
					columnBuilder.append("\"");
				} else {
					columnBuilder.append(columnName);
				}
				columnBuilder.append(" ");
				columnBuilder.append(columnTypeName.toUpperCase());
				if (columnTypeName.equals("DECIMAL")) {
					if (columnSize > 0) {
						columnBuilder.append("(");
						columnBuilder.append(columnSize);
						if (decimalDigits > 0) {
							columnBuilder.append(", ");
							columnBuilder.append(decimalDigits);
						}
						columnBuilder.append(")");
					}								
				} else if ((columnTypeName.equals("CHAR") || columnTypeName.equals("VARCHAR") || columnTypeName.equals("NCHAR") || columnTypeName.equals("NVARCHAR"))) {
					if (columnSize > 0) {
						columnBuilder.append("(");
						columnBuilder.append(columnSize);
						columnBuilder.append(")");
					}
				}
				columnBuilder.append(" ");
				columnBuilder.append(isNullable.toUpperCase());

				columnListBuilder.put(columnBuilder.toString());
				processedColumns.add(columnName);
			}
		}		
		return columnListBuilder.toString(1);
	}

	private final String getIsNullableClause(String clause) {
		clause  = clause.toLowerCase();
		switch(clause) {
		case "0":
		case "n":	
		case "no":
		case "false":
		case "not null":	
			return "not null";
		case "1":
		case "y":	
		case "yes":
		case "true":
		case "null":
			return "null";

		}
		//Return empty string if cannot recognize ( to be safe)
		return "";
	}

	private void createMetadataTables(Path metadataFilePath) throws ServletException {
		String url = "jdbc:sqlite:" + metadataFilePath;
		String createMetadataTableSql = "CREATE TABLE IF NOT EXISTS src_object_info(object_name TEXT PRIMARY KEY, object_type TEXT, allowed_columns TEXT, unique_key_columns TEXT, incremental_key_columns TEXT, group_name TEXT, group_position INTEGER, mask_columns TEXT, delete_condition TEXT, select_conditions TEXT, enable INTEGER)";
		try (Connection conn = DriverManager.getConnection(url)){
			try (Statement stmt = conn.createStatement()) {
				stmt.execute(createMetadataTableSql);
			}

			//
			//Create additional metadata tables here.
			//Upgrade codepath should be here and in LoadJob.
			//
			try (Statement stmt = conn.createStatement()) {
				stmt.execute("CREATE TABLE IF NOT EXISTS src_object_reload_configurations(object_name TEXT PRIMARY KEY, reload_schema_on_next_restart INT, reload_schema_on_each_restart INT, reload_object_on_next_restart INT, reload_object_on_each_restart INT)");
			}
		} catch(SQLException e) {
			this.globalTracer.error("Failed to create dbreader metadata table : " + e.getMessage(), e);
			throw new ServletException("Failed to create dbreader metadata table : " + e.getMessage(), e);
		}
	}

	private final void addSrcObjectInfoToMetadataTable(Path metadataFilePath, List<ObjectInfo> tiList) throws ServletException {
		String url = "jdbc:sqlite:" + metadataFilePath;
		String metadataTableInsertOrUpdateSql = "INSERT INTO src_object_info(object_name, object_type, allowed_columns, unique_key_columns, incremental_key_columns, group_name, group_position, mask_columns, delete_condition, select_conditions, enable) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (object_name) DO UPDATE SET object_type = excluded.object_type, allowed_columns = excluded.allowed_columns, unique_key_columns = excluded.unique_key_columns";
		String metadataTableInsertOrIgnoreSql = "INSERT OR IGNORE INTO src_object_info(object_name, object_type, allowed_columns, unique_key_columns, incremental_key_columns, group_name, group_position, mask_columns, delete_condition, select_conditions, enable) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,?)"; 

		try (Connection conn = DriverManager.getConnection(url)){
			conn.setAutoCommit(false);		
			try (
					PreparedStatement pInsertOrUpdateStmt = conn.prepareStatement(metadataTableInsertOrUpdateSql);
					PreparedStatement pInsertOrIgnoreStmt = conn.prepareStatement(metadataTableInsertOrIgnoreSql)) {			

				boolean insertOrUpdateBatchPopulated = false;
				boolean insertOrIgnoreBatchPopulated = false;
				for (ObjectInfo ti : tiList) {
					if (ti.reloadObjectSchemas) {
						pInsertOrUpdateStmt.setString(1, ti.objectName);
						pInsertOrUpdateStmt.setString(2, ti.objectType);
						pInsertOrUpdateStmt.setString(3, ti.columnList);
						pInsertOrUpdateStmt.setString(4, ti.pkColList);
						pInsertOrUpdateStmt.setString(5, "");
						pInsertOrUpdateStmt.setString(6, "");
						pInsertOrUpdateStmt.setInt(7, 1);
						pInsertOrUpdateStmt.setString(8, "");
						pInsertOrUpdateStmt.setString(9, "");
						pInsertOrUpdateStmt.setString(10, "");
						pInsertOrUpdateStmt.setInt(11, 0);
						pInsertOrUpdateStmt.addBatch();
						insertOrUpdateBatchPopulated = true;
					} else {
						pInsertOrIgnoreStmt.setString(1, ti.objectName);
						pInsertOrIgnoreStmt.setString(2, ti.objectType);
						pInsertOrIgnoreStmt.setString(3, ti.columnList);
						pInsertOrIgnoreStmt.setString(4, ti.pkColList);
						pInsertOrUpdateStmt.setString(5, "");
						pInsertOrUpdateStmt.setString(6, "");
						pInsertOrUpdateStmt.setInt(7, 1);
						pInsertOrUpdateStmt.setString(8, "");
						pInsertOrUpdateStmt.setString(9, "");
						pInsertOrUpdateStmt.setString(10, "");
						pInsertOrUpdateStmt.setInt(11, 0);
						pInsertOrIgnoreStmt.addBatch();
						insertOrIgnoreBatchPopulated = true;
					}
				}				

				if (insertOrUpdateBatchPopulated) {
					pInsertOrUpdateStmt.executeBatch();
				}
				if (insertOrIgnoreBatchPopulated) {
					pInsertOrIgnoreStmt.executeBatch();
				}
			}
			conn.commit();
		} catch (SQLException e) {
			this.globalTracer.error("Failed to insert object info in metadata file : " + metadataFilePath + " : " + e.getMessage(), e);
			throw new ServletException("Failed to insert object info in metadata file : " + metadataFilePath + " : " + e.getMessage() , e);
		}
	}


	private final void validateConnection(String srcConnectionString, String user, String password) throws ServletException {
		if (user == null) {
			try(Connection conn = DriverManager.getConnection(srcConnectionString)) {
				//Do nothing
			} catch (SQLException e){
				this.globalTracer.error("Failed to connect to source database. Please verify specified connection string : ", e);
				throw new ServletException("Failed to connect to source database. Please verify specified connection string : " +  e.getMessage(), e);
			}
		} else {
			try(Connection conn = DriverManager.getConnection(srcConnectionString, user, password)) {
				//Do nothing
			} catch (SQLException e){
				this.globalTracer.error("Failed to connect to source database. Please verify specified connection string : ", e);
				throw new ServletException("Failed to connect to source database. Please verify specified connection string : " +  e.getMessage(), e);
			}
		}
	}

	private final void validateDBPath(Path dbPath) throws ServletException {
		if (dbPath == null) {
			this.globalTracer.error("Invalid connection string specified. Please verify specified connection string.");
			throw new ServletException("Invalid connection string specified. Please verify specified connection string.");			
		}
		Path parentDir = dbPath.getParent();
		if (parentDir.toFile().exists()) {
			if (!parentDir.toFile().canRead()) {
				this.globalTracer.error("The directory specified in the connection string is not readable : " + parentDir + ". Please verify specified connection string.");
				throw new ServletException("The directory specified in the connection string is not readable : " + parentDir + ". Please verify specified connection string.");
			}
		} else {
			this.globalTracer.error("The directory specified in the connection string is invalid. Please verify specified connection string.");
			throw new ServletException("The directory specified in the connection string is invalid. Please verify specified connection string.");
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


	private final boolean isDatabaseAllowed(SrcType srcType) {
		switch(srcType) {
		case DUCKDB:
			return true;
		case CSV:
			return false;
		case MYSQL:
			return false;
		case POSTGRESQL:
			return true;
		case SQLITE:
			return false;
		default:
			return false;
		}
	}

	public final boolean isSchemaAllowed(SrcType srcType) {
		switch(srcType) {
		case DUCKDB:			
			return true;
		case CSV:
			return false;
		case MYSQL:
			return true;
		case POSTGRESQL:
			return true;
		case SQLITE:
			return false;
		default:
			return false;
		}
	}

}


