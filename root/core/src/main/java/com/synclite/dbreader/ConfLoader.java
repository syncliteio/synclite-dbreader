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

package com.synclite.dbreader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class ConfLoader {

	private static final class InstanceHolder {
		private static ConfLoader INSTANCE = new ConfLoader();
	}

	public static ConfLoader getInstance() {
		return InstanceHolder.INSTANCE;
	}

	private HashMap<String, String> properties;
	private Path syncLiteDeviceDir;
	private Path syncLiteLoggerConfigurationFile;
	private String srcConnectionString;
	private String srcConnectionInitializationStmt;
	private Long srcConnectionTimeoutS;
	private Long srcDBReaderIntervalS;
	private Boolean dbReaderStopAfterFirstIteration;
	private Long srcDBReaderBatchSize;
	private Integer srcDBReaderProcessors;
	private DBReaderMethod srcDBReaderMethod;
	private Boolean srcInferSchemaChanges;
	private Boolean srcInferObjectDrop;
	private Boolean srcInferObjectCreate;
	private Boolean srcReloadObjectSchemas;
	private Boolean srcReloadObjectSchemasOnEachJobRestart;
	private Boolean srcReloadObjects;
	private Boolean srcReloadObjectsOnEachJobRestart;
	private String srcDefaultUniqueKeyColumnList;
	private String srcDefaultIncrementalKeyColumnList;
	private String srcDefaultSoftDeleteCondition;
	private String srcDefaultMaskColumnList;
	private String srcQueryTimestampConversionFunction;	
	private String srcTimestampIncrementalKeyInitialValue;
	private String srcNumericIncrementalKeyInitialValue;
	private SrcType srcType;
	private String srcDatabase;
	private String srcSchema;
	private String srcDBLink;
	private String srcUser;
	private String srcPassword;
	private ObjectType srcObjectType;
	private MetadataReadMethod srcObjectMetadataReadMethod;
	private MetadataReadMethod srcColumnMetadataReadMethod;
	private MetadataReadMethod srcConstraintMetadataReadMethod;
	private TraceLevel traceLevel;
	private Long updateStatisticsIntervalS;
	private Boolean enableStatisticsCollector; 
	private String srcNumericValueMask;
	private String srcAlphabeticValueMask;
	private Long srcDBReaderObjectRecordLimit;
	private String srcObjectNamePattern;
	private Boolean srcReadNullIncrementalKeyRecords;
	private Boolean srcComputeMaxIncrementalKeyInDB;
	private Boolean srcQuoteObjectNames;
	private Boolean srcQuoteColumnNames;
	private Boolean srcUseCatalogScopeResolution;
	private Boolean srcUseSchemaScopeResolution;
	private Boolean srcCsvFilesWithHeaders;
	private Character srcCSVFilesFieldDelimiter;
	private String srcCSVFilesRecordDelimiter;
	private Character srcCSVFilesEscapeCharacter;
	private Character srcCSVFilesQuoteCharacter;
	private String srcCSVFilesNullString;
	private Boolean srcCsvFilesIgnoreEmptyLines;
	private Boolean srcCsvFilesTrimFields;
	private FileStorageType srcFileStorageType;
	private Path srcFileStorageLocalFSDirectory;
	private String srcFileStorageSFTPHost;
	private Integer srcFileStorageSFTPPort;
	private String srcFileStorageSFTPDirectory;
	private String srcFileStorageSFTPUser;
	private String srcFileStorageSFTPPassword;
	private String srcFileStorageS3Url;
	private String srcFileStorageS3BucketName;
	private String srcFileStorageS3AccessKey;
	private String srcFileStorageS3SecretKey;
	private Long failedObjectRetryIntervalS;
	private Boolean retryFailedObjects;
	private Path licenseFilePath;
	private SyncLiteEdition edition;
	private Set<SrcAppType> allowedSourceAppTypes;
	private Set<SrcType> allowedSources;

	public final Path getSyncLiteDeviceDir() {
		return syncLiteDeviceDir;
	}

	public final Path getSyncLiteLoggerConfigurationFile() {
		return syncLiteLoggerConfigurationFile;
	}

	public final String getSrcConnectionString() {
		return srcConnectionString;
	}

	public final String getSrcConnectionInitializationStmt() {
		return srcConnectionInitializationStmt;
	}

	public final String getSrcDatabase() {
		return srcDatabase;
	}

	public final String getSrcSchema() {
		return srcSchema;
	}

	public final String getSrcDBLink() {
		return srcDBLink;
	}

	public final String getSrcUser() {
		return srcUser;
	}

	public final String getSrcPassword() {
		return srcPassword;
	}

	public final SrcType getSrcType() {
		return srcType;
	}
	
	public final ObjectType getSrcObjectType() {
		return this.srcObjectType;
	}

	public final String getSrcObjectNamePattern() {
		return this.srcObjectNamePattern;
	}
	
	public final MetadataReadMethod getSrcObjectMetadataReadMethod() {
		return this.srcObjectMetadataReadMethod;
	}

	public final MetadataReadMethod getSrcColumnMetadataReadMethod() {
		return this.srcColumnMetadataReadMethod;
	}

	public final MetadataReadMethod getSrcConstraintMetadataReadMethod() {
		return this.srcConstraintMetadataReadMethod;
	}

	public final Long getSrcConnectionTimeoutS() {
		return srcConnectionTimeoutS;
	}

	public final Long getSrcDBReaderIntervalS() {
		return srcDBReaderIntervalS;
	}

	public final Boolean getDBReaderStopAfterFirstIteration() {
		return dbReaderStopAfterFirstIteration;
	}
	
	public final Long getFailedObjectRetryIntervalS() {
		return failedObjectRetryIntervalS;
	}

	public final Boolean getRetryFailedObjects() {
		return retryFailedObjects;
	}

	public final Long getSrcDBReaderBatchSize() {
		return srcDBReaderBatchSize;
	}

	public final Integer getSrcDBReaderProcessors() {
		return srcDBReaderProcessors;
	}

	public final DBReaderMethod getSrcDBReaderMethod() {
		return srcDBReaderMethod;
	}
	
	public final Boolean getSrcInferSchemaChanges() {
		return srcInferSchemaChanges;
	}

	public final Boolean getSrcInferObjectDrop() {
		return srcInferObjectDrop;
	}

	public final Boolean getSrcInferObjectCreate() {
		return srcInferObjectCreate;
	}

	public final Boolean getSrcReloadObjectSchemas() {
		return srcReloadObjectSchemas || srcReloadObjectSchemasOnEachJobRestart;
	}

	public final Boolean getSrcReadNullIncrementalKeyRecords() {
		return this.srcReadNullIncrementalKeyRecords;
	}

	public final Boolean getSrcComputeMaxIncrementalKeyInDB() {
		return this.srcComputeMaxIncrementalKeyInDB;
	}

	public final Boolean getSrcQuoteObjectNames() {
		return this.srcQuoteObjectNames;
	}

	public final Boolean getSrcQuoteColumnNames() {
		return this.srcQuoteColumnNames;
	}

	public final Boolean getSrcUseCatalogScopeResolution() {
		return this.srcUseCatalogScopeResolution;
	}

	public final Boolean getSrcUseSchemaScopeResolution() {
		return this.srcUseSchemaScopeResolution;
	}
	
	public final Boolean getSrcReloadObjects() {
		return srcReloadObjects || srcReloadObjectsOnEachJobRestart;
	}

	public final String getSrcDefaultUniqueKeyColumnList() {
		return srcDefaultUniqueKeyColumnList;
	}

	public final String getSrcDefaultIncrementalKeyColumnList() {
		return srcDefaultIncrementalKeyColumnList;
	}

	public final String getSrcDefaultSoftDeleteCondition() {
		return srcDefaultSoftDeleteCondition;
	}

	public final String getSrcQueryTimestampConversionFunction() {
		return srcQueryTimestampConversionFunction;
	}

	public final String getSrcTimestampIncrementalKeyInitialValue() {
		return srcTimestampIncrementalKeyInitialValue;
	}

	public final String getSrcNumericIncrementalKeyInitialValue() {
		return srcNumericIncrementalKeyInitialValue;
	}

	public final Long getSrcDBReaderObjectRecordLimit() {
		return this.srcDBReaderObjectRecordLimit;
	}

	public final String getSrcDefaultMaskColumnList() {
		return srcDefaultMaskColumnList;
	}

	public final Boolean getSrcCsvFilesWithHeaders() {
		return this.srcCsvFilesWithHeaders;
	}
	
	public Character getSrcCSVFilesFieldDelimiter() {
		return srcCSVFilesFieldDelimiter;
	}
	
	public String getSrcCSVFilesRecordDelimiter() {
		return srcCSVFilesRecordDelimiter;
	}
	
	public Character getSrcCSVFilesEscapeCharacter() {
		return srcCSVFilesEscapeCharacter;
	}
	
	public Character getSrcCSVFilesQuoteCharacter() {
		return srcCSVFilesQuoteCharacter;
	}
	
	public String getSrcCSVFilesNullString() {
		return srcCSVFilesNullString;
	}
	
	public Boolean getSrcCsvFilesIgnoreEmptyLines() {
		return srcCsvFilesIgnoreEmptyLines;
	}
	
	public Boolean getSrcCsvFilesTrimFields() {
		return srcCsvFilesTrimFields;
	}
	
	public FileStorageType getSrcFileStorageType() {
		return this.srcFileStorageType;
	}
	
	public Path getSrcFileStorageLocalFSDirectory() {
		return this.srcFileStorageLocalFSDirectory;
	}
	
	public String getSrcFileStorageSFTPHost() {
		return this.srcFileStorageSFTPHost;
	}
	
	public Integer getSrcFileStorageSFTPPort() {
		return this.srcFileStorageSFTPPort;
	}
	
	public String getSrcFileStorageSFTPDirectory() {
		return this.srcFileStorageSFTPDirectory;
	}
	
	public String getSrcFileStorageSFTPUser() {
		return this.srcFileStorageSFTPUser;
	}
	
	public String getSrcFileStorageSFTPPassword() {
		return srcFileStorageSFTPPassword;
	}
	
	public String getSrcFileStorageS3Url() {
		return this.srcFileStorageS3Url;
	}
	
	public String getSrcFileStorageS3BucketName() {
		return this.srcFileStorageS3BucketName;
	}
	
	public String getSrcFileStorageS3AccessKey() {
		return this.srcFileStorageS3AccessKey;
	}
	
	public String getSrcFileStorageS3SecretKey() {
		return this.srcFileStorageS3SecretKey;
	}

	public Path getLicenseFile() {
		return this.licenseFilePath;
	}
	
	public SyncLiteEdition getConsolidatorEdition() {
		return edition;
	}

	private ConfLoader() {

	}

	public void loadDBReaderConfigProperties(Path propsPath) throws SyncLiteException {
		this.properties = loadPropertiesFromFile(propsPath);
		validateAndLoadLicense();
		validateAndProcessProperties();    	
	}
	
	public void loadDBReaderArgProperties(Path dbReaderArgFilePath) throws SyncLitePropsException {
		HashMap<String, String> args = loadPropertiesFromFile(dbReaderArgFilePath);
		this.properties.putAll(args);
		validateAndProcessArgProperties();		
	}

	private void validateAndLoadLicense() throws SyncLiteException {
		String propValue = properties.get("license-file");
		if (propValue != null) {
			this.licenseFilePath= Path.of(propValue);
			if (this.licenseFilePath == null) {
				throw new SyncLitePropsException("Invalid value specified for license-file in configuration file");
			}
			if (!Files.exists(this.licenseFilePath)) {
				throw new SyncLitePropsException("Specified license-file does not exist : " + licenseFilePath);
			}
			if (!this.licenseFilePath.toFile().canRead()) {
				throw new SyncLitePropsException("No read permission on specified license-file path");
			}
		} else {
			//throw new SyncLitePropsException("license-file not specified in configuration file");
		}
		LicenseVerifier.validateLicense(licenseFilePath);
		properties.putAll(LicenseVerifier.getLicenseProperties());
	}



	public static HashMap<String, String> loadPropertiesFromFile(Path propsPath) throws SyncLitePropsException {
		BufferedReader reader = null;
		try {
			HashMap<String, String> properties = new HashMap<String, String>();
			reader = new BufferedReader(new FileReader(propsPath.toFile()));
			String line = reader.readLine();
			while (line != null) {
				line = line.trim();
				if (line.trim().isEmpty()) {
					line = reader.readLine();
					continue;
				}
				if (line.startsWith("#")) {
					line = reader.readLine();
					continue;
				}
				String[] tokens = line.split("=", 2);
				if (tokens.length < 2) {
					if (tokens.length == 1) {
						if (tokens[0].startsWith("=")) {
							throw new SyncLitePropsException("Invalid line in configuration file " + propsPath + " : " + line);
						}
					} else { 
						throw new SyncLitePropsException("Invalid line in configuration file " + propsPath + " : " + line);
					}
				}
				properties.put(tokens[0].trim().toLowerCase(), line.substring(line.indexOf("=") + 1, line.length()).trim());
				line = reader.readLine();
			}
			return properties;
		} catch (IOException e) {
			throw new SyncLitePropsException("Failed to load configuration file : " + propsPath + " : ", e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					throw new SyncLitePropsException("Failed to close configuration file : " + propsPath + ": " , e);
				}
			}
		}
	}

	private void validateAndProcessProperties() throws SyncLitePropsException {
		String propValue = properties.get("synclite-device-dir");
		if (propValue != null) {
			this.syncLiteDeviceDir = Path.of(propValue);
			if (this.syncLiteDeviceDir == null) {
				throw new SyncLitePropsException("Invalid value specified for synclite-device-dir in configuration file");
			}
			if (!Files.exists(this.syncLiteDeviceDir)) {
				throw new SyncLitePropsException("Specified device-data-root path does not exist : " + this.syncLiteDeviceDir);
			}
			if (!this.syncLiteDeviceDir.toFile().canRead()) {
				throw new SyncLitePropsException("No read permission on specified synclite-device-dir path");
			}
			if (!this.syncLiteDeviceDir.toFile().canWrite()) {
				throw new SyncLitePropsException("No write permission on specified synclite-device-dir path");
			}
		} else {
			throw new SyncLitePropsException("synclite-device-dir not specified in configuration file");
		}

		propValue = properties.get("edition");
		if (propValue != null) {
			try {
				this.edition = SyncLiteEdition.valueOf(propValue);
				if (this.edition == null) {
					throw new SyncLitePropsException("Invalid edition " + propValue + " specified : in license file");
				}
			} catch (IllegalArgumentException e) {
				throw new SyncLitePropsException("Invalid edition " + propValue + " specified in license file");
			}
		} else {
			this.edition = SyncLiteEdition.DEVELOPER;
		}

		propValue = properties.get("synclite-logger-configuration-file");
		if (propValue != null) {
			this.syncLiteLoggerConfigurationFile= Path.of(propValue);
			if (this.syncLiteLoggerConfigurationFile == null) {
				throw new SyncLitePropsException("Invalid value specified for synclite-logger-configuration-file in configuration file");
			}
			if (!Files.exists(this.syncLiteLoggerConfigurationFile)) {
				throw new SyncLitePropsException("Specified synclite-logger-configuration-file does not exist : " + syncLiteLoggerConfigurationFile);
			}
			if (!this.syncLiteLoggerConfigurationFile.toFile().canRead()) {
				throw new SyncLitePropsException("No read permission on specified synclite-logger-configuration-file path");
			}
		} else {
			throw new SyncLitePropsException("synclite-logger-configuration-file not specified in configuration file");
		}

		propValue = properties.get("src-type");
		if (propValue != null) {
			try {
				this.srcType  = SrcType.valueOf(propValue);
				if (this.srcType == null) {
					throw new SyncLitePropsException("Unsupported src-type specified : " + propValue);
				}
				
				if (this.edition == SyncLiteEdition.DEVELOPER) {
					if ((this.srcType == SrcType.POSTGRESQL) || (this.srcType == SrcType.SQLITE) || (this.srcType == SrcType.DUCKDB) || (this.srcType == SrcType.MONGODB)) {
						//Allowed in Developer Edition
					} else {
						throw new SyncLitePropsException("Feature Not Supported : Source " + this.srcType + " is not supported in developer edition.");
					}
				}

			} catch (IllegalArgumentException e) {
				throw new SyncLitePropsException("Invalid src-type specified : " + propValue);
			}
		} else {
			throw new SyncLitePropsException("src-type not specified in configuration file");
		}

		propValue = properties.get("src-database");
		if (propValue != null) {
			this.srcDatabase = propValue;
		} else {
			if (isDatabaseAllowed()) {
				throw new SyncLitePropsException("src-database is not specified in configuration file");
			}
		}

		propValue = properties.get("src-schema");
		if (propValue != null) {
			this.srcSchema= propValue;
		} else {
			if (isSchemaAllowed()) {
				throw new SyncLitePropsException("src-schema is not specified in configuration file");
			}
		}

		propValue = properties.get("src-connection-string");
		if (propValue != null) {
			this.srcConnectionString = propValue;
		} else {
			throw new SyncLitePropsException("src-connection-string not specified in configuration file");
		}

		propValue = properties.get("src-connection-initialization-stmt");
		if ((propValue != null) && (!propValue.isBlank())) {
			this.srcConnectionInitializationStmt = propValue;
		} else {
			this.srcConnectionInitializationStmt = null;
		}

		propValue = properties.get("src-dblink");
		if ((propValue != null) && (!propValue.isBlank())) {
			this.srcDBLink = propValue.strip();
		} 

		propValue = properties.get("src-user");
		if (propValue != null) {
			this.srcUser = propValue;
		} 

		propValue = properties.get("src-password");
		if (propValue != null) {
			this.srcPassword = propValue;
		} 

		propValue = properties.get("src-connection-timeout-s");
		if (propValue != null) {
			try {
				this.srcConnectionTimeoutS = Long.valueOf(propValue);
				if (this.srcConnectionTimeoutS == null) {
					throw new SyncLitePropsException("Invalid value specified for src-connection-timeout-s in configuration file");
				} else if (this.srcConnectionTimeoutS <= 0) {
					throw new SyncLitePropsException("Please specify a positive numeric value for src-connection-timeout-s in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Please specify a positive numeric value for src-connection-timeout-s in configuration file");
			}
		} else {
			this.srcConnectionTimeoutS = 30L;
		}

		propValue = properties.get("src-dbreader-interval-s");
		if (propValue != null) {
			try {
				this.srcDBReaderIntervalS = Long.valueOf(propValue);
				if (this.srcDBReaderIntervalS == null) {
					throw new SyncLitePropsException("Invalid value specified for src-dbreader-interval-s in configuration file");
				} else if (this.srcDBReaderIntervalS <= 0) {
					throw new SyncLitePropsException("Please specify a positive numeric value for src-dbreader-interval-s in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Please specify a positive numeric value for src-dbreader-interval-s in configuration file");
			}
		} else {
			this.srcDBReaderIntervalS = 300L;
		}

		propValue = properties.get("dbreader-stop-after-first-iteration");
		if (propValue != null) {
			try {
				this.dbReaderStopAfterFirstIteration = Boolean.valueOf(propValue);
				if (this.srcDBReaderIntervalS == null) {
					throw new SyncLitePropsException("Invalid value specified for dbreader-stop-after-first-iteration in configuration file");
				} else if (this.srcDBReaderIntervalS <= 0) {
					throw new SyncLitePropsException("Please specify a positive numeric value for dbreader-stop-after-first-iteration in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Please specify a valid boolean value for dbreader-stop-after-first-iteration in configuration file");
			}
		} else {
			this.dbReaderStopAfterFirstIteration = false;
		}

		propValue = properties.get("src-dbreader-batch-size");
		if (propValue != null) {
			try {
				this.srcDBReaderBatchSize= Long.valueOf(propValue);
				if (this.srcDBReaderBatchSize == null) {
					throw new SyncLitePropsException("Invalid value specified for src-dbreader-batch-size in configuration file");
				} else if (this.srcDBReaderBatchSize <= 0) {
					throw new SyncLitePropsException("Please specify a positive numeric value for src-dbreader-batch-size in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Please specify a positive numeric value for src-dbreader-batch-size in configuration file");
			}
		} else {
			this.srcDBReaderBatchSize = 100000L;
		}

		propValue = properties.get("src-dbreader-processors");
		if (propValue != null) {
			try {
				this.srcDBReaderProcessors = Integer.valueOf(propValue);
				if (this.srcDBReaderProcessors== null) {
					throw new SyncLitePropsException("Invalid value specified for src-dbreader-processors in configuration file");
				} else if (this.srcDBReaderProcessors <= 0) {
					throw new SyncLitePropsException("Please specify a positive numeric value for src-dbreader-processors in configuration file");
				}
			} catch(NumberFormatException e) {
				throw new SyncLitePropsException("Please specify a positive numeric value for src-dbreader-processors in configuration file");
			}
		} else {
			this.srcDBReaderProcessors = Runtime.getRuntime().availableProcessors();
		}

		propValue = properties.get("src-dbreader-method");
		if (propValue != null) {
			try {
				this.srcDBReaderMethod = DBReaderMethod.valueOf(propValue);
				if (this.srcDBReaderMethod == DBReaderMethod.LOG_BASED) {
					if (this.srcType != SrcType.MONGODB) {
						throw new SyncLitePropsException("src-dbreader-method : " + this.srcDBReaderMethod + " not supported for src type : " + this.srcType);
					}
				}
			} catch (IllegalArgumentException e) {
				throw new SyncLitePropsException("Please specify a valid value for src-dbreader-method in configuration file");
			}
		} else {
			this.srcDBReaderMethod = DBReaderMethod.INCREMENTAL;
		}

		propValue = properties.get("src-object-type");
		if (propValue != null) {
			this.srcObjectType = ObjectType.valueOf(propValue);
			if (this.srcObjectType == null) {
				throw new SyncLitePropsException("Invalid value specified for src-object-type in configuration file");
			}
		} else {
			this.srcObjectType = ObjectType.TABLE;
		}

		propValue = properties.get("src-object-name-pattern");
		if (propValue != null) {
			this.srcObjectNamePattern = propValue;
		}

		propValue = properties.get("src-object-metadata-read-method");
		if (propValue != null) {
			this.srcObjectMetadataReadMethod = MetadataReadMethod.valueOf(propValue);
			if (this.srcObjectMetadataReadMethod == null) {
				throw new SyncLitePropsException("Invalid value specified for src-object-metadata-read-method in configuration file");
			}
		} else {
			this.srcObjectMetadataReadMethod = MetadataReadMethod.NATIVE;
		}

		propValue = properties.get("src-column-metadata-read-method");
		if (propValue != null) {
			this.srcColumnMetadataReadMethod = MetadataReadMethod.valueOf(propValue);
			if (this.srcColumnMetadataReadMethod == null) {
				throw new SyncLitePropsException("Invalid value specified for src-column-metadata-read-method in configuration file");
			}
		} else {
			this.srcColumnMetadataReadMethod = MetadataReadMethod.NATIVE;
		}

		propValue = properties.get("src-constraint-metadata-read-method");
		if (propValue != null) {
			this.srcConstraintMetadataReadMethod = MetadataReadMethod.valueOf(propValue);
			if (this.srcConstraintMetadataReadMethod == null) {
				throw new SyncLitePropsException("Invalid value specified for src-constraint-metadata-read-method in configuration file");
			}
		} else {
			this.srcConstraintMetadataReadMethod = MetadataReadMethod.NATIVE;
		}

		propValue = properties.get("src-infer-schema-changes");
		if (propValue != null) {
			try {
				this.srcInferSchemaChanges = Boolean.valueOf(propValue);
				if (this.srcInferSchemaChanges == null) {
					throw new SyncLitePropsException("Invalid value specified for src-infer-schema-changes in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Invalid value specified for src-infer-schema-changes in configuration file");
			}
		} else {
			this.srcInferSchemaChanges = false;
		}

		propValue = properties.get("src-infer-object-drop");
		if (propValue != null) {
			try {
				this.srcInferObjectDrop = Boolean.valueOf(propValue);
				if (this.srcInferObjectDrop == null) {
					throw new SyncLitePropsException("Invalid value specified for src-infer-object-drop in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Invalid value specified for src-infer-object-drop in configuration file");
			}
		} else {
			this.srcInferObjectDrop = false;
		}

		propValue = properties.get("src-infer-object-create");
		if (propValue != null) {
			try {
				this.srcInferObjectCreate = Boolean.valueOf(propValue);
				if (this.srcInferObjectCreate == null) {
					throw new SyncLitePropsException("Invalid value specified for src-infer-object-create in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Invalid value specified for src-infer-object-create in configuration file");
			}
		} else {
			this.srcInferObjectCreate = false;
		}

		propValue = properties.get("src-reload-objects");
		if (propValue != null) {
			try {
				this.srcReloadObjects = Boolean.valueOf(propValue);
				if (this.srcReloadObjects == null) {
					throw new SyncLitePropsException("Invalid value specified for src-reload-objects in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Invalid value specified for src-reload-objects in configuration file");
			}
		} else {
			this.srcReloadObjects = false;
		}

		propValue = properties.get("src-reload-objects-on-each-job-restart");
		if (propValue != null) {
			try {
				this.srcReloadObjectsOnEachJobRestart = Boolean.valueOf(propValue);
				if (this.srcReloadObjectsOnEachJobRestart == null) {
					throw new SyncLitePropsException("Invalid value specified for src-reload-objects-on-each-job-restart in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Invalid value specified for src-reload-objects-on-each-job-restart in configuration file");
			}
		} else {
			this.srcReloadObjectsOnEachJobRestart = false;
		}

		propValue = properties.get("src-reload-object-schemas");
		if (propValue != null) {
			try {
				this.srcReloadObjectSchemas = Boolean.valueOf(propValue);
				if (this.srcReloadObjectSchemas == null) {
					throw new SyncLitePropsException("Invalid value specified for src-reload-object-schemas in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Invalid value specified for src-reload-object-schemas in configuration file");
			}
		} else {
			this.srcReloadObjectSchemas = false;
		}

		propValue = properties.get("src-reload-object-schemas-on-each-job-restart");
		if (propValue != null) {
			try {
				this.srcReloadObjectSchemasOnEachJobRestart = Boolean.valueOf(propValue);
				if (this.srcReloadObjectSchemasOnEachJobRestart == null) {
					throw new SyncLitePropsException("Invalid value specified for src-reload-object-schemas-on-each-job-restart in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Invalid value specified for src-reload-object-schemas-on-each-job-restart in configuration file");
			}
		} else {
			this.srcReloadObjectSchemasOnEachJobRestart = false;
		}

		propValue = properties.get("src-default-unique-key-column-list");
		if (propValue != null) {
			this.srcDefaultUniqueKeyColumnList = propValue;
		} else {
			this.srcDefaultUniqueKeyColumnList = null;
		}

		propValue = properties.get("src-default-incremental-key-column-list");
		if (propValue != null) {
			this.srcDefaultIncrementalKeyColumnList = propValue;
		} else {
			this.srcDefaultIncrementalKeyColumnList = null;
		}

		propValue = properties.get("src-default-soft-delete-condition");
		if (propValue != null) {
			this.srcDefaultSoftDeleteCondition = propValue;
		} else {
			this.srcDefaultSoftDeleteCondition = null;
		}

		propValue = properties.get("src-query-timestamp-conversion-function");
		if ((propValue != null) && (!propValue.isBlank())) {
			this.srcQueryTimestampConversionFunction = propValue;
		} else {
			this.srcQueryTimestampConversionFunction = null;
		}

		propValue = properties.get("src-timestamp-incremental-key-initial-value");
		if ((propValue != null) && (!propValue.isBlank())) {
			this.srcTimestampIncrementalKeyInitialValue = propValue;
		} else {
			this.srcTimestampIncrementalKeyInitialValue = "0001-01-01 00:00:00";
		}

		propValue = properties.get("src-numeric-incremental-key-initial-value");
		if ((propValue != null) && (!propValue.isBlank())) {
			this.srcNumericIncrementalKeyInitialValue = propValue;
		} else {
			this.srcNumericIncrementalKeyInitialValue = "0";
		}
		
		propValue = properties.get("src-default-mask-column-list");
		if (propValue != null) {
			this.srcDefaultMaskColumnList = propValue;
		} else {
			this.srcDefaultMaskColumnList = null;
		}

		propValue = properties.get("src-numeric-value-mask");
		if (propValue != null) {
			this.srcNumericValueMask= propValue;
			if (this.srcNumericValueMask.length() != 1) {
				throw new SyncLitePropsException("Invalid value specified for src-numeric-value-mask in configuration file, please specify a single digit");
			} 
			if (!this.srcNumericValueMask.matches("[0-9]")) {
				throw new SyncLitePropsException("Invalid value specified for src-numeric-value-mask in configuration file, please specify a single digit");
			}
		} else {
			this.srcNumericValueMask = "9";
		}

		
		propValue = properties.get("src-alphabetic-value-mask");
		if (propValue != null) {
			this.srcAlphabeticValueMask = propValue;
			if (this.srcAlphabeticValueMask.length() != 1) {
				throw new SyncLitePropsException("Invalid value specified for src-alphabetic-value-mask in configuration file, please specify a single alphabetic character");
			} 
			if (!this.srcAlphabeticValueMask.matches("[a-zA-Z]")) {
				throw new SyncLitePropsException("Invalid value specified for src-numeric-value-mask in configuration file, please specify a single alphabetic character");
			}
		} else {
			this.srcAlphabeticValueMask = "X";
		}

		
		propValue = properties.get("src-dbreader-object-record-limit");
		if (propValue != null) {
			try {
				this.srcDBReaderObjectRecordLimit = Long.valueOf(propValue);
				if (this.srcDBReaderObjectRecordLimit == null) {
					throw new SyncLitePropsException("Invalid value specified for src-dbreader-object-record-limit in configuration file");
				} else if (this.srcDBReaderObjectRecordLimit < 0) {
					throw new SyncLitePropsException("Please specify a non-negative numeric value for src-dbreader-object-record-limit in configuration file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Please specify a positive numeric value for src-dbreader-object-record-limit in configuration file");
			}
		} else {
			this.srcDBReaderObjectRecordLimit = 0L;
		}

		propValue = properties.get("src-read-null-incremental-key-records");
		if (propValue != null) {
			try {
				this.srcReadNullIncrementalKeyRecords = Boolean.valueOf(propValue);
				if (this.srcReadNullIncrementalKeyRecords == null) {
					throw new SyncLitePropsException("Invalid value specified for src-read-null-incremental-key-records in configuration file");
				} 
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Please specify a valid boolean value for src-read-null-incremental-key-records in configuration file");
			}
		} else {
			this.srcReadNullIncrementalKeyRecords = false;
		}

		propValue = properties.get("src-compute-max-incremental-key-in-db");
		if (propValue != null) {
			try {
				this.srcComputeMaxIncrementalKeyInDB = Boolean.valueOf(propValue);
				if (this.srcComputeMaxIncrementalKeyInDB == null) {
					throw new SyncLitePropsException("Invalid value specified for src-compute-max-incremental-key-in-db in configuration file");
				} 
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Please specify a valid boolen value for src-compute-max-incremental-key-in-db in configuration file");
			}
		} else {
			this.srcComputeMaxIncrementalKeyInDB = true;
		}

		propValue = properties.get("src-quote-object-names");
		if (propValue != null) {
			try {
				this.srcQuoteObjectNames = Boolean.valueOf(propValue);
				if (this.srcQuoteObjectNames == null) {
					throw new SyncLitePropsException("Invalid value specified for src-quote-object-names in configuration file");
				} 
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Please specify a positive numeric value for src-quote-object-names in configuration file");
			}
		} else {
			this.srcQuoteObjectNames = false;
		}

		propValue = properties.get("src-quote-column-names");
		if (propValue != null) {
			try {
				this.srcQuoteColumnNames = Boolean.valueOf(propValue);
				if (this.srcQuoteColumnNames == null) {
					throw new SyncLitePropsException("Invalid value specified for src-quote-column-names in configuration file");
				} 
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Please specify a positive numeric value for src-quote-column-names in configuration file");
			}
		} else {
			this.srcQuoteColumnNames = false;
		}

		propValue = properties.get("src-use-catalog-scope-resolution");
		if (propValue != null) {
			try {
				this.srcUseCatalogScopeResolution = Boolean.valueOf(propValue);
				if (this.srcUseCatalogScopeResolution == null) {
					throw new SyncLitePropsException("Invalid value specified for src-use-catalog-scope-resolution in configuration file");
				} 
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Please specify a positive numeric value for src-use-catalog-scope-resolution in configuration file");
			}
		} else {
			this.srcUseCatalogScopeResolution = true;
		}

		propValue = properties.get("src-use-schema-scope-resolution");
		if (propValue != null) {
			try {
				this.srcUseSchemaScopeResolution = Boolean.valueOf(propValue);
				if (this.srcUseSchemaScopeResolution == null) {
					throw new SyncLitePropsException("Invalid value specified for src-use-schema-scope-resolution in configuration file");
				} 
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Please specify a positive numeric value for src-use-schema-scope-resolution in configuration file");
			}
		} else {
			this.srcUseSchemaScopeResolution = true;
		}

		if (srcType == SrcType.CSV) {
			propValue = properties.get("src-csv-files-with-headers");
			if (propValue != null) {
				try {
					this.srcCsvFilesWithHeaders = Boolean.valueOf(propValue);
					if (this.srcCsvFilesWithHeaders == null) {
						throw new SyncLitePropsException("Invalid value specified for src-csv-files-with-headers in configuration file");
					} 
				} catch (NumberFormatException e) {
					throw new SyncLitePropsException("Please specify a valid boolean value for src-csv-files-with-headers in configuration file");
				}
			} else {
				this.srcCsvFilesWithHeaders = true;
			}
			

			propValue = properties.get("src-csv-files-field-delimiter");
			if (propValue != null) {
				if (propValue.length() != 1) {
					throw new SyncLitePropsException("Invalid value specified for src-csv-files-field-delimiter in configuration file");
				}
				this.srcCSVFilesFieldDelimiter = propValue.charAt(0);
			} else {
				this.srcCSVFilesFieldDelimiter = ',';
			}

			propValue = properties.get("src-csv-files-record-delimiter");
			if (propValue != null) {
				this.srcCSVFilesRecordDelimiter = propValue;
			} else {
				this.srcCSVFilesRecordDelimiter = "\r\n";
			}

			propValue = properties.get("src-csv-files-escape-character");
			if (propValue != null) {
				if (propValue.length() != 1) {
					throw new SyncLitePropsException("Invalid value specified for src-csv-files-escape-character in configuration file");
				}
				this.srcCSVFilesEscapeCharacter = propValue.charAt(0);
			} else {
				this.srcCSVFilesEscapeCharacter = '"';
			}
			
			propValue = properties.get("src-csv-files-quote-character");
			if (propValue != null) {
				if (propValue.length() != 1) {
					throw new SyncLitePropsException("Invalid value specified for src-csv-files-quote-character in configuration file");
				}
				this.srcCSVFilesQuoteCharacter = propValue.charAt(0);
			} else {
				this.srcCSVFilesQuoteCharacter = '"';
			}
			
			propValue = properties.get("src-csv-files-null-string");
			if (propValue != null) {
				this.srcCSVFilesNullString = propValue;
			} else {
				this.srcCSVFilesNullString = "null";
			}
			
			propValue = properties.get("src-csv-files-ignore-empty-lines");
			if (propValue != null) {
				try {
					this.srcCsvFilesIgnoreEmptyLines = Boolean.valueOf(propValue);
					if (this.srcCsvFilesIgnoreEmptyLines == null) {
						throw new SyncLitePropsException("Invalid value specified for src-csv-files-ignore-empty-lines in configuration file");
					} 
				} catch (NumberFormatException e) {
					throw new SyncLitePropsException("Please specify a positive numeric value for src-csv-files-ignore-empty-lines in configuration file");
				}
			} else {
				this.srcCsvFilesIgnoreEmptyLines = true;
			}


			propValue = properties.get("src-csv-files-trim-fields");
			if (propValue != null) {
				try {
					this.srcCsvFilesTrimFields = Boolean.valueOf(propValue);
					if (this.srcCsvFilesTrimFields == null) {
						throw new SyncLitePropsException("Invalid value specified for src-csv-files-trim-fields in configuration file");
					} 
				} catch (NumberFormatException e) {
					throw new SyncLitePropsException("Please specify a positive numeric value for src-csv-files-trim-fields in configuration file");
				}
			} else {
				this.srcCsvFilesTrimFields = false;
			}

			if (this.srcType == SrcType.CSV) {
				propValue = properties.get("src-file-storage-type");
				if (propValue != null) {
					try {
						this.srcFileStorageType = FileStorageType.valueOf(propValue);
						if (this.srcFileStorageType == null) {
							throw new SyncLitePropsException("Invalid value specified for src-file-storage-type in configuration file");
						} 
					} catch (IllegalArgumentException e) {
						throw new SyncLitePropsException("Please specify a a valid value for src-file-storage-type in configuration file");
					}
				} else {
					throw new SyncLitePropsException("src-file-storage-type must be specified when source type is CSV");
				}
				
				propValue = properties.get("src-file-storage-local-fs-directory");
				if ((propValue == null) || (propValue.isBlank())) {
					throw new SyncLitePropsException("Please specify src-file-storage-local-fs-directory");
				}
				this.srcFileStorageLocalFSDirectory = Path.of(propValue);
				if (!Files.exists(this.srcFileStorageLocalFSDirectory)) {
					throw new SyncLitePropsException("Specified src-file-storage-local-fs-directory does not exist : " + srcFileStorageLocalFSDirectory);
				}
				
				if (!this.srcFileStorageLocalFSDirectory.toFile().canRead()) {
					throw new SyncLitePropsException("Specified src-file-storage-local-fs-directory does not have read access : " + srcFileStorageLocalFSDirectory);
				}

				if (this.srcFileStorageType == FileStorageType.SFTP) {
					propValue = properties.get("src-file-storage-sftp-host");
					if ((propValue == null) || (propValue.isBlank())) {
						throw new SyncLitePropsException("Please specify src-file-storage-sftp-host");
					}
					this.srcFileStorageSFTPHost = propValue;
					
					propValue = properties.get("src-file-storage-sftp-port");
					if ((propValue == null) || (propValue.isBlank())) {
						throw new SyncLitePropsException("Please specify src-file-storage-sftp-port");
					}
					try {
						this.srcFileStorageSFTPPort = Integer.valueOf(propValue);
						if (this.srcFileStorageSFTPPort <= 0) {
							throw new SyncLitePropsException("Please a valid positive numeric value for src-file-storage-sftp-port");
						}
					} catch (NumberFormatException e) {
						throw new SyncLitePropsException("Please a valid positive numeric value for src-file-storage-sftp-port"); 
					}

					propValue = properties.get("src-file-storage-sftp-directory");
					if ((propValue == null) || (propValue.isBlank())) {
						throw new SyncLitePropsException("Please specify src-file-storage-sftp-directory");
					}
					this.srcFileStorageSFTPDirectory = propValue;
					
					propValue = properties.get("src-file-storage-sftp-user");
					if ((propValue == null) || (propValue.isBlank())) {
						throw new SyncLitePropsException("Please specify src-file-storage-sftp-user");
					}
					this.srcFileStorageSFTPUser = propValue;

					propValue = properties.get("src-file-storage-sftp-password");
					if ((propValue == null) || (propValue.isBlank())) {
						throw new SyncLitePropsException("Please specify src-file-storage-sftp-password");
					}
					this.srcFileStorageSFTPPassword = propValue;

				} else if (this.srcFileStorageType == FileStorageType.S3) {

					propValue = properties.get("src-file-storage-s3-url");
					if ((propValue == null) || (propValue.isBlank())) {
						throw new SyncLitePropsException("Please specify src-file-storage-s3-url");
					}
					this.srcFileStorageS3Url = propValue;
					
					propValue = properties.get("src-file-storage-s3-bucket-name");
					if ((propValue == null) || (propValue.isBlank())) {
						throw new SyncLitePropsException("Please specify src-file-storage-s3-bucket-name");
					}
					this.srcFileStorageS3BucketName = propValue;
					
					propValue = properties.get("src-file-storage-s3-access-key");
					if ((propValue == null) || (propValue.isBlank())) {
						throw new SyncLitePropsException("Please specify src-file-storage-s3-access-key");
					}
					this.srcFileStorageS3AccessKey = propValue;
					
					propValue = properties.get("src-file-storage-s3-secret-key");
					if ((propValue == null) || (propValue.isBlank())) {
						throw new SyncLitePropsException("Please specify src-file-storage-s3-secret-key");
					}
					this.srcFileStorageS3SecretKey = propValue;
				}				
			}			
		}
		
		propValue = properties.get("dbreader-trace-level");
		if (propValue != null) {
			this.traceLevel= TraceLevel.valueOf(propValue);
			if (this.traceLevel == null) {
				throw new SyncLitePropsException("Invalid value specified for dbreader-trace-level in configuration file");
			}
		} else {
			traceLevel = TraceLevel.DEBUG;
		}
		
		propValue = properties.get("dbreader-update-statistics-interval-s");
		if (propValue != null) {
			this.updateStatisticsIntervalS = Long.valueOf(propValue);
			if (this.updateStatisticsIntervalS == null) {
				throw new SyncLitePropsException("Invalid value specified for dbreader-update-statistics-interval-s in configuration file");
			}
		} else {
			this.updateStatisticsIntervalS = 5L;
		}

		propValue = properties.get("dbreader-enable-statistics-collector");
		if (propValue != null) {
			this.enableStatisticsCollector =  Boolean.valueOf(propValue);
			if (this.enableStatisticsCollector == null) {
				throw new SyncLitePropsException("Invalid value specified for dbreader-enable-statistics-collector in configuration file");
			}
		} else {
			this.enableStatisticsCollector = true;
		}

		propValue = properties.get("dbreader-retry-failed-objects");
		if (propValue != null) {
			this.retryFailedObjects  = Boolean.valueOf(propValue);
			if (this.retryFailedObjects == null) {
				throw new SyncLitePropsException("Invalid value specified for dbreader-retry-failed-objects in configuration file");
			}
		} else {
			this.retryFailedObjects = true;
		}

		propValue = properties.get("dbreader-failed-object-retry-interval-s");
		if (propValue != null) {
			this.failedObjectRetryIntervalS = Long.valueOf(propValue);
			if (this.failedObjectRetryIntervalS == null) {
				throw new SyncLitePropsException("Invalid value specified for dbreader-failed-object-retry-interval-s in configuration file");
			}
		} else {
			this.failedObjectRetryIntervalS = 10L;
		}

		
		propValue = properties.get("allowed-sources");
		this.allowedSources= new HashSet<SrcType>();
		if (propValue != null) {
			String allowedSrcs[]= propValue.split(",");
			SrcType allowedSrc;
			for (String allowedSrcName : allowedSrcs) {
				try {
					allowedSrc = SrcType.valueOf(allowedSrcName);
					if (allowedSrc == null) {
						//throw new SyncLitePropsException("Invalid value specified in allowed-sources in license file : " + allowedDstName);
						//Ignore unsupported dst type
					}
					allowedSources.add(allowedSrc);
				} catch (IllegalArgumentException e) {
					//throw new SyncLitePropsException("Invalid value specified in allowed-sources in license file : " + allowedDstName);
					//Ignore unsupported src type from here.
				}
			}

			if (!allowedSources.contains(SrcType.ALL)) {
				if (!allowedSources.contains(srcType)) {
					throw new SyncLitePropsException("Specified source " + srcType + " not allowed with your license");
				}				
			}        
		} else {
			//Optional field
			//Ignore
		}
		
		propValue = properties.get("allowed-source-app-types");
		this.allowedSourceAppTypes = new HashSet<SrcAppType>();
		if (propValue != null) {
			String allowedSrcAppTypes[]= propValue.split(",");
			SrcAppType allowedSrcApp;
			for (String allowedSrcAppName : allowedSrcAppTypes) {
				try {
					allowedSrcApp = SrcAppType.valueOf(allowedSrcAppName);
					if (allowedSrcApp == null) {
						//throw new SyncLitePropsException("Invalid value specified in allowed-source-app-types in license file : " + allowedDstName);
						//Ignore unsupported dst type
					}
					allowedSourceAppTypes.add(allowedSrcApp);
				} catch (IllegalArgumentException e) {
					//throw new SyncLitePropsException("Invalid value specified in allowed-source-app-types in license file : " + allowedDstName);
					//Ignore unsupported src type from here.
				}
			}

			if (!allowedSourceAppTypes.contains(SrcAppType.ALL)) {
				if (!allowedSourceAppTypes.contains(SrcAppType.DBREADER)) {
					throw new SyncLitePropsException("SyncLite DBReader app type not allowed with your license");
				}				
			}        
		} else {
			throw new SyncLitePropsException("allowed-source-app-types not specified in license file");
		}
	}

	private void validateAndProcessArgProperties() throws SyncLitePropsException {
		String propValue = properties.get("src-reload-objects");
		if (propValue != null) {
			try {
				this.srcReloadObjects = Boolean.valueOf(propValue);
				if (this.srcReloadObjects == null) {
					throw new SyncLitePropsException("Invalid value specified for src-reload-objects in arguments file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Invalid value specified for src-reload-objects in arguments file");
			}
		} else {
			this.srcReloadObjects = false;
		}

		propValue = properties.get("src-reload-object-schemas");
		if (propValue != null) {
			try {
				this.srcReloadObjectSchemas = Boolean.valueOf(propValue);
				if (this.srcReloadObjectSchemas == null) {
					throw new SyncLitePropsException("Invalid value specified for src-reload-object-schemas in arguments file");
				}
			} catch (NumberFormatException e) {
				throw new SyncLitePropsException("Invalid value specified for src-reload-object-schemas in configuration file");
			}
		} else {
			this.srcReloadObjectSchemas = false;
		}	
	}
	
	private final String getSrcName(SrcType sType) {
		switch (sType) {
		case DUCKDB:
			return "DuckDB";
		case MYSQL:
			return "MySQL";
		case POSTGRESQL:
			return "PostgreSQL";
		case SQLITE:
			return "SQLite";
		default:
			return sType.toString();
		}
	}

	private final boolean isDatabaseAllowed() {
		switch(srcType) {
		case DUCKDB:
			return true;
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

	public final boolean isSchemaAllowed() {
		switch(srcType) {
		case DUCKDB:
			return true;
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

	public TraceLevel getTraceLevel() {
		return this.traceLevel;
	}

	public String getSrcNumericValueMask() {
		return srcNumericValueMask;
	}
	
	public String getSrcAlphabeticValueMask() {
		return srcAlphabeticValueMask;
	}

	public long getUpdateStatisticsIntervalS() {
		return this.updateStatisticsIntervalS;
	}

	public boolean getEnableStatisticsCollector() {
		return this.enableStatisticsCollector;
	}

	public String getSrcName() {
		switch(this.srcType) {
		case DUCKDB:
			return "DuckDB";
		case MYSQL:
			return "MySQL";
		case POSTGRESQL:
			return "POSTGRESQL";
		case SQLITE:
			return "SQLITE";		
		}
		return this.srcType.toString();
	}

}
