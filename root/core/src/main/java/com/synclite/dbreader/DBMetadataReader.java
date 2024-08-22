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

import java.io.FileOutputStream;
import java.io.OutputStream;
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
import java.util.Vector;

import org.apache.log4j.Logger;
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
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

public class DBMetadataReader {
	
	private static Logger tracer;
	
	static void setLogger(Logger tr) {
		tracer = tr;
	}
	
	protected static ObjectInfo readSrcSchema(String objName) throws SyncLiteException {
		if (ConfLoader.getInstance().getSrcColumnMetadataReadMethod() == MetadataReadMethod.JDBC) {
			tracer.info("Using JDBC method for reading column metadata for object : " + objName);
			return readSrcSchemaDefault(objName);
		} else {
			tracer.info("Using NATIVE method for reading column metadata for object : " + objName);
			switch (ConfLoader.getInstance().getSrcType()) {
			case CSV:
				return readSrcSchemaCSV(objName);
			case MONGODB:
				return readSrcSchemaMongoDB(objName);
			case POSTGRESQL:
				return readSrcSchemaPG(objName);
			case MYSQL:	
				return readSrcSchemaMySQL(objName);
			default:
				return readSrcSchemaDefault(objName);
			}
		}
	}
	
	private static ObjectInfo readSrcSchemaCSV(String objName) throws SyncLiteException {
		throw new SyncLiteException("Infer schema not supported for CSV source");
	}

	//MAKE SURE THAT THE IMPLEMENTATION HERE IS EXACTLY IDENTICAL TO THAT IN ValidateDBReader
	private static ObjectInfo readSrcSchemaDefault(String objName) throws SyncLiteException {
		ObjectInfo tblInfo = null;
		try (Connection connection = JDBCConnector.getInstance().connect()) {
			DatabaseMetaData metaData = connection.getMetaData();

			//
			// Get a list of all object names in the database
			String catalog = ConfLoader.getInstance().getSrcDatabase();
			String schema = ConfLoader.getInstance().getSrcSchema();
			if ((catalog == null) && (schema != null)) {
				catalog = schema;
			}
			if ((catalog != null) && (schema == null)) {
				schema = catalog;
			}

			String[] objTypes = null;
			switch(ConfLoader.getInstance().getSrcObjectType()) {
			case TABLE:
				objTypes = new String[] {"TABLE"};
				break;
			case VIEW:
				objTypes = new String[] {"VIEW"};
				break;
			case ALL :
				objTypes = new String[] {"TABLE", "VIEW"};
			}

			try (ResultSet tables = metaData.getTables(catalog, schema, objName, objTypes)) {
				if (tables.next()) {
					JSONArray columnListBuilder = new JSONArray();
					//Get a list of column names and data types for the current table
					String tableName = tables.getString("TABLE_NAME");
					HashMap<String, String> colDefMap = new HashMap<String, String>();

					try (ResultSet columns = metaData.getColumns(catalog, schema, tableName, null)) {
						HashSet<String> processedCols = new HashSet<String>();
						while (columns.next()) {
							String columnName = columns.getString("COLUMN_NAME");
							if (processedCols.contains(columnName)) {
								continue;
							}

							StringBuilder columnTypeBuilder = new StringBuilder();
							String columnTypeName = columns.getString("TYPE_NAME");
							int columnSize = columns.getInt("COLUMN_SIZE");
							int decimalDigits = columns.getInt("DECIMAL_DIGITS");

							String isNullable = getIsNullableClause(columns.getString("IS_NULLABLE")); // NULL or NOT NULL
							columnTypeBuilder.append(columnTypeName.toUpperCase());
							if (columnTypeName.equals("DECIMAL")) {
								if (columnSize > 0) {
									columnTypeBuilder.append("(");
									columnTypeBuilder.append(columnSize);
									columnTypeName += "(" + columnSize;
									if (decimalDigits > 0) {
										columnTypeBuilder.append(", ");
										columnTypeBuilder.append(decimalDigits);
										columnTypeName += ", " + decimalDigits;
									}
									columnTypeBuilder.append(")");
									columnTypeName += ")";
								}								
							} else if ((columnTypeName.equals("CHAR") || columnTypeName.equals("VARCHAR") || columnTypeName.equals("NCHAR") || columnTypeName.equals("NVARCHAR"))) {
								if (columnSize > 0) {
									columnTypeBuilder.append("(");
									columnTypeBuilder.append(columnSize);
									columnTypeBuilder.append(")");
									columnTypeName += "(" + columnSize + ")";
								}
							}

							String fullColType = columnTypeBuilder.toString() + " " + isNullable.toUpperCase();
							colDefMap.put(columnName.toUpperCase(),  fullColType);		
							if (columnName.matches(".*\\s+.*")) {
								columnListBuilder.put("\"" + columnName + "\"" + " " + fullColType);
							} else {
								columnListBuilder.put(columnName + " " + fullColType);
							}

							processedCols.add(columnName);
						}
					}

					String pkColList = getPKColList(connection, catalog, schema, tableName);
					tblInfo = new ObjectInfo(objName, columnListBuilder.toString(1), pkColList, colDefMap);					
				}
			}
		} catch (SQLException e) {
			tracer.error("Failed to read object metadata from source database :", e);
			throw new SyncLiteException("Failed to read object schema for object " + objName + " from source database : ", e);
		}
		return tblInfo;
	}

	private static String getPKColList(Connection conn, String catalog, String schema, String objectName) throws SQLException {
		if (ConfLoader.getInstance().getSrcConstraintMetadataReadMethod() == MetadataReadMethod.JDBC) {
			tracer.info("Using JDBC method for reading constraint metadata for object : " + objectName);
			return getPKColListDefault(conn, catalog, schema, objectName);
		} else {
			tracer.info("Using NATIVE method for reading constraint metadata for object : " + objectName);
			switch (ConfLoader.getInstance().getSrcType()) {
			case MYSQL:
				return getPKColListMySQL(conn, catalog, schema, objectName);
			case POSTGRESQL:
				return getPKColListPG(conn, catalog, schema, objectName);
			default:
				return getPKColListDefault(conn, catalog, schema, objectName);
			}		
		}	
	}

	private static String getPKColListNativeInternal(Connection conn, String catalog, String schema, String objectName, String pkConsColsSql, String ukConsSql, String ukConsColsSql) throws SQLException {
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

	private static final String getPKColListPG(Connection conn, String catalog, String schema, String objectName) throws SQLException {
		String pkConsColsSql = "SELECT kcu.column_name FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_catalog = tc.constraint_catalog AND kcu.constraint_schema = tc.constraint_schema AND kcu.constraint_name = tc.constraint_name WHERE kcu.table_catalog = ? AND kcu.table_schema = ? AND kcu.table_name = ? AND tc.constraint_type = 'PRIMARY KEY' ORDER BY kcu.ordinal_position";

		String ukConsSql = "SELECT constraint_name FROM information_schema.table_constraints WHERE table_catalog = ? AND table_schema = ? AND table_name = ? AND constraint_type = 'UNIQUE' ORDER BY constraint_name";

		String ukConsColsSql = "SELECT kcu.column_name	FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_catalog = tc.constraint_catalog AND kcu.constraint_schema = tc.constraint_schema AND kcu.constraint_name = tc.constraint_name WHERE kcu.table_catalog = ? AND kcu.table_schema = ? AND kcu.table_name = ? AND tc.constraint_name = ? ORDER BY kcu.ordinal_position";

		return getPKColListNativeInternal(conn, catalog, schema, objectName, pkConsColsSql, ukConsSql, ukConsColsSql);
	}

	private static final String getPKColListMySQL(Connection conn, String catalog, String schema, String objectName) throws SQLException {
		String pkConsColsSql = "SELECT column_name FROM information_schema.key_column_usage WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY' ORDER BY ordinal_position";

		String ukConsSql = "SELECT constraint_name FROM information_schema.table_constraints WHERE table_schema = ? AND table_name = ? AND constraint_type = 'UNIQUE' ORDER BY constraint_name";

		String ukConsColsSql = "SELECT column_name FROM information_schema.key_column_usage WHERE table_schema = ? AND table_name = ? AND constraint_name = ? ORDER BY ordinal_position";

		return getPKColListNativeInternal(conn, null, schema, objectName, pkConsColsSql, ukConsSql, ukConsColsSql);
	}

	private static final String getPKColListDefault(Connection conn, String catalog, String schema, String objectName) throws SQLException {
		StringBuilder pkColListBuilder = new StringBuilder();
		DatabaseMetaData metaData = conn.getMetaData();
		try (ResultSet tables = metaData.getTables(catalog, schema, objectName, new String[] { "TABLE" })) {
			//Get the primary key columns for the current object
			HashSet<String> processedPKCols = new HashSet<String>();
			try (ResultSet primaryKeys = metaData.getPrimaryKeys(catalog, schema, objectName)) {
				boolean first = true;
				while (primaryKeys.next()) {
					String primaryKeyColumnName = primaryKeys.getString("COLUMN_NAME");
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
				try (ResultSet uniqueKeys = metaData.getIndexInfo(catalog, schema, objectName, true, false)) {
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
		}
		return pkColListBuilder.toString();
	}

	//MAKE SURE THAT THE IMPLEMENTATION HERE IS EXACTLY IDENTICAL TO THAT IN ValidateDBReader
	private static ObjectInfo readSrcSchemaPG(String objName) throws SyncLiteException {
		ObjectInfo tblInfo = null;
		try (Connection connection = JDBCConnector.getInstance().connect()) {
			// Get a list of all object names in the database
			String catalog = ConfLoader.getInstance().getSrcDatabase();
			String schema = ConfLoader.getInstance().getSrcSchema();

			try (PreparedStatement stmt = connection.prepareStatement(
					"SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, " +
							"IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE, UDT_NAME " +
							"FROM INFORMATION_SCHEMA.COLUMNS " +
							"WHERE TABLE_CATALOG = ? AND TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
					)) {
				stmt.setString(1, catalog);
				stmt.setString(2, schema);
				stmt.setString(3, objName);
				ResultSet columnInfo = stmt.executeQuery();

				if (columnInfo.next()) {
					JSONArray columnListBuilder = new JSONArray();
					HashMap<String, String> colDefMap = new HashMap<String, String>();

					HashSet<String> processedCols = new HashSet<String>();
					do {
						String columnName = columnInfo.getString("COLUMN_NAME");
						if (processedCols.contains(columnName)) {
							continue;
						}
						String columnTypeName = columnInfo.getString("DATA_TYPE");
						int characterMaxLength = columnInfo.getInt("CHARACTER_MAXIMUM_LENGTH");
						String isNullable = getIsNullableClause(columnInfo.getString("IS_NULLABLE")); // NULL or NOT NULL
						int dataPrecision = columnInfo.getInt("NUMERIC_PRECISION");
						int dataScale = columnInfo.getInt("NUMERIC_SCALE");

						String udtName = columnInfo.getString("UDT_NAME");

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

						StringBuilder columnTypeBuilder = new StringBuilder();
						// Append column name, data type, precision, scale, and data length for applicable types

						columnTypeBuilder.append(columnTypeName.toUpperCase());
						if (isPGNumericWithPrecisionScaleDataType(columnTypeName)) {
							if (dataPrecision > 0) {
								columnTypeBuilder.append("(");
								columnTypeBuilder.append(dataPrecision);
								if (dataScale > 0) {
									columnTypeBuilder.append(",");
									columnTypeBuilder.append(dataScale);
								}
								columnTypeBuilder.append(")");
							}
						} else if (!columnTypeName.toUpperCase().equals("TEXT") && (characterMaxLength > 0)) {
							columnTypeBuilder.append("(");
							columnTypeBuilder.append(characterMaxLength);
							columnTypeBuilder.append(")");
							columnTypeName = columnTypeName + "(" + characterMaxLength + ")";						
						}

						String fullColType = columnTypeBuilder.toString() + " " + isNullable.toUpperCase();
						colDefMap.put(columnName.toUpperCase(),  fullColType);		
						if (columnName.matches(".*\\s+.*")) {
							columnListBuilder.put("\"" + columnName + "\"" + " " + fullColType);
						} else {
							columnListBuilder.put(columnName + " " + fullColType);
						}

						processedCols.add(columnName);
					} while (columnInfo.next());

					String pkColList = getPKColList(connection, catalog, schema, objName);

					tblInfo = new ObjectInfo(objName, columnListBuilder.toString(1), pkColList, colDefMap);
				}
			}
		} catch (SQLException e) {
			tracer.error("Failed to read object metadata from source database :", e);
			throw new SyncLiteException("Failed to read object schema for object " + objName + " from the source database : ", e);
		}
		return tblInfo;
	}

	//MAKE SURE THAT THE IMPLEMENTATION HERE IS EXACTLY IDENTICAL TO THAT IN ValidateDBReader
	private static ObjectInfo readSrcSchemaMySQL(String objName) throws SyncLiteException {
		ObjectInfo tblInfo = null;
		try (Connection connection = JDBCConnector.getInstance().connect()) {
			String schema = ConfLoader.getInstance().getSrcSchema();
			String catalog = schema;

			try (PreparedStatement stmt = connection.prepareStatement(
					"SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, " +
							"IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE " +
							"FROM INFORMATION_SCHEMA.COLUMNS " +
							"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
					)) {
				stmt.setString(1, schema);
				stmt.setString(2, objName);
				ResultSet columnInfo = stmt.executeQuery();

				if (columnInfo.next()) {
					JSONArray columnListBuilder = new JSONArray();
					HashMap<String, String> colDefMap = new HashMap<String, String>();

					HashSet<String> processedCols = new HashSet<String>();
					do {
						String columnName = columnInfo.getString("COLUMN_NAME");
						if (processedCols.contains(columnName)) {
							continue;
						}
						String columnTypeName = columnInfo.getString("DATA_TYPE");
						int characterMaxLength = columnInfo.getInt("CHARACTER_MAXIMUM_LENGTH");
						String isNullable = getIsNullableClause(columnInfo.getString("IS_NULLABLE")); // NULL or NOT NULL
						int dataPrecision = columnInfo.getInt("NUMERIC_PRECISION");
						int dataScale = columnInfo.getInt("NUMERIC_SCALE");

						StringBuilder columnTypeBuilder = new StringBuilder();
						// Append column name, data type, precision, scale, and data length for applicable types

						columnTypeBuilder.append(columnTypeName.toUpperCase());
						if (isMySQLNumericWithPrecisionScaleDataType(columnTypeName)) {
							if (dataPrecision > 0) {
								columnTypeBuilder.append("(");
								columnTypeBuilder.append(dataPrecision);
								if (dataScale > 0) {
									columnTypeBuilder.append(",");
									columnTypeBuilder.append(dataScale);
								}
								columnTypeBuilder.append(")");
							}
						} else if (!columnTypeName.toUpperCase().equals("TEXT") && (characterMaxLength > 0)) {
							columnTypeBuilder.append("(");
							columnTypeBuilder.append(characterMaxLength);
							columnTypeBuilder.append(")");
							columnTypeName = columnTypeName + "(" + characterMaxLength + ")";						
						}

						String fullColType = columnTypeBuilder.toString() + " " + isNullable.toUpperCase();
						colDefMap.put(columnName.toUpperCase(),  fullColType);		
						if (columnName.matches(".*\\s+.*")) {
							columnListBuilder.put("\"" + columnName + "\"" + " " + fullColType);
						} else {
							columnListBuilder.put(columnName + " " + fullColType);
						}

						processedCols.add(columnName);
					} while (columnInfo.next());

					String pkColList = getPKColList(connection, catalog, schema, objName);

					tblInfo = new ObjectInfo(objName, columnListBuilder.toString(1), pkColList, colDefMap);
				}
			}
		} catch (SQLException e) {
			tracer.error("Failed to read object metadata from source database :", e);
			throw new SyncLiteException("Failed to read object schema for object " + objName + " from the source database : ", e);
		}
		return tblInfo;
	}

	private static final boolean isSQLServerNumericWithPrecisionScaleDataType(String columnTypeName) {
		return (columnTypeName.equalsIgnoreCase("NUMERIC") || columnTypeName.equalsIgnoreCase("DECIMAL"));
	}

	private static final boolean isPGNumericWithPrecisionScaleDataType(String columnTypeName) {
		return (columnTypeName.equalsIgnoreCase("NUMERIC") || columnTypeName.equalsIgnoreCase("DECIMAL"));
	}

	private static final boolean isMySQLNumericWithPrecisionScaleDataType(String columnTypeName) {
		return (columnTypeName.equalsIgnoreCase("NUMERIC") || columnTypeName.equalsIgnoreCase("DECIMAL"));
	}

	private static ObjectInfo readSrcSchemaMongoDB(String objName) throws SyncLiteException {
		ObjectInfo tblInfo = null;
		try {
			MongoClient mongoClient = MongoDBReader.getMongoClient();
			MongoDatabase mongoDatabase = mongoClient.getDatabase(ConfLoader.getInstance().getSrcDatabase());
			boolean collectionExists = mongoDatabase.listCollectionNames().into(new ArrayList<>()).contains(objName);
			if (collectionExists) {
				HashMap<String, String> colDefMap = new HashMap<String, String>();
				JSONArray columnListBuilder = new JSONArray();
				StringBuilder columnBuilder = new StringBuilder();
				columnBuilder.append("_id ObjectId NOT NULL");
				columnListBuilder.put(columnBuilder.toString());
				columnBuilder = new StringBuilder();
				columnBuilder.append("document BSON NOT NULL");
				columnListBuilder.put(columnBuilder.toString());

				colDefMap.put("_ID", "ObjectId NOT NULL");
				colDefMap.put("DOCUMENT", "BSON NOT NULL");
				String pkColList = "_id";
				tblInfo = new ObjectInfo(objName, columnListBuilder.toString(1), pkColList, colDefMap);
			}
		} catch (Exception e) {
			tracer.error("Failed to read object metadata from source database :", e);
			throw new SyncLiteException("Failed to read object schema for object " + objName + " from the source database : ", e);
		}
		return tblInfo;
	}

	private final static String getIsNullableClause(String clause) {
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


	public static HashMap<String, String> loadObjectNamesFromMetadata() throws SyncLiteException {
		if (ConfLoader.getInstance().getSrcObjectMetadataReadMethod() == MetadataReadMethod.JDBC) {
			//this.globalTracer.info("Using JDBC method for reading object metadata");
			return loadObjectNamesFromMetadataDefault();
		} else {
			//this.globalTracer.info("Using NATIVE method for reading object metadata");
			switch (ConfLoader.getInstance().getSrcType()) {
			case CSV:
				return loadObjectNamesFromMetadataCSV();
			case MONGODB:
				return loadObjectNamesFromMetadataMongoDB();
			case MYSQL:
				return loadObjectNamesFromMetadataMySQL ();
			case POSTGRESQL:
				return loadObjectNamesFromMetadataPG();
			default:
				return loadObjectNamesFromMetadataDefault();
			}
		}
	}

	private static HashMap<String, String> loadObjectNamesFromMetadataPG() throws SyncLiteException {
		// Get a list of all object names in the database
		HashMap<String, String> objectMap= new HashMap<String, String>();
		String catalog = ConfLoader.getInstance().getSrcDatabase();
		String schema = ConfLoader.getInstance().getSrcSchema();
		String objectNamePattern = ConfLoader.getInstance().getSrcObjectNamePattern();

		String sql = "";
		switch(ConfLoader.getInstance().getSrcObjectType()) {		
		case TABLE:
			sql = "SELECT TABLE_NAME, 'TABLE' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '" + catalog + "' AND TABLE_SCHEMA = '" + schema + "' AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME LIKE '" + objectNamePattern + "'";
			break;
		case VIEW:
			sql = "SELECT TABLE_NAME, 'VIEW' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '" + catalog + "' AND TABLE_SCHEMA = '" + schema + "' AND TABLE_TYPE = 'VIEW' AND TABLE_NAME LIKE '" + objectNamePattern + "'";
			break;
		case ALL:
			String tableSql = "SELECT TABLE_NAME, 'TABLE' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '" + catalog + "' AND TABLE_SCHEMA = '" + schema + "' AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME LIKE '" + objectNamePattern + "'";
			String viewSql = "SELECT TABLE_NAME, 'VIEW' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '" + catalog + "' AND TABLE_SCHEMA = '" + schema + "' AND TABLE_TYPE = 'VIEW' AND TABLE_NAME LIKE '" + objectNamePattern + "'";
			sql = tableSql + " UNION " + viewSql;
			break;
		}

		try (Connection conn = JDBCConnector.getInstance().connect()) {
			try (Statement stmt = conn.createStatement()) {
				try (ResultSet rs = stmt.executeQuery(sql)) {
					while(rs.next()) {
						String tabName = rs.getString(1);
						String tabType = rs.getString(2);
						objectMap.put(tabName, tabType);
					}
				}
			}
		}  catch (Exception e) {
			throw new SyncLiteException("Failed to read object info from source database : " + e.getMessage(), e);
		}
		return objectMap;
	}

	private static HashMap<String, String> loadObjectNamesFromMetadataMySQL() throws SyncLiteException {
		// Get a list of all object names in the database
		HashMap<String, String> objectMap = new HashMap<String, String>();
		String schema = ConfLoader.getInstance().getSrcSchema();
		String objectNamePattern = ConfLoader.getInstance().getSrcObjectNamePattern();

		String sql = "";
		switch(ConfLoader.getInstance().getSrcObjectType()) {		
		case TABLE:
			sql = "SELECT TABLE_NAME, 'TABLE' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema + "' AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME LIKE '" + objectNamePattern + "'";
			break;
		case VIEW:
			sql = "SELECT TABLE_NAME, 'VIEW' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema + "' AND TABLE_TYPE = 'VIEW' AND TABLE_NAME LIKE '" + objectNamePattern + "'";
			break;
		case ALL:
			String tableSql = "SELECT TABLE_NAME, 'TABLE' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema + "' AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME LIKE '" + objectNamePattern + "'";
			String viewSql = "SELECT TABLE_NAME, 'VIEW' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema + "' AND TABLE_TYPE = 'VIEW' AND TABLE_NAME LIKE '" + objectNamePattern + "'";
			sql = tableSql + " UNION " + viewSql;
			break;
		}

		try (Connection conn = JDBCConnector.getInstance().connect()) {
			try (Statement stmt = conn.createStatement()) {
				try (ResultSet rs = stmt.executeQuery(sql)) {
					while(rs.next()) {
						String tabName = rs.getString(1);
						String tabType = rs.getString(2);
						objectMap.put(tabName, tabType);
					}
				}
			}
		} catch (Exception e) {
			throw new SyncLiteException("Failed to read object info from source database : " + e.getMessage(), e);
		} 
		return objectMap;		
	}
	
	private static HashMap<String, String> loadObjectNamesFromMetadataMongoDB() throws SyncLiteException {
		HashMap<String, String> objectMap= new HashMap<String, String>();
		try {
			try (MongoClient mongoClient = MongoDBReader.getMongoClient()) {
				MongoDatabase db = mongoClient.getDatabase(ConfLoader.getInstance().getSrcDatabase());
				for (String collectionName : db.listCollectionNames()) {
					objectMap.put(collectionName, "TABLE");
				}
			}
		} catch (Exception e) {
			throw new SyncLiteException("Failed to get collection names from source MongoDB database : " + e.getMessage(), e);
		}
		return objectMap;
	}

	
	private static HashMap <String, String> loadObjectNamesFromMetadataDefault() throws SyncLiteException {
		// Get a list of all table names in the database
		String[] objTypes = null;
		String catalog = ConfLoader.getInstance().getSrcDatabase();
		String schema = ConfLoader.getInstance().getSrcSchema();
		String objectNamePattern = ConfLoader.getInstance().getSrcObjectNamePattern();

		try (Connection conn = JDBCConnector.getInstance().connect()) {
			DatabaseMetaData metaData = conn.getMetaData();
			HashMap<String, String> objectMap= new HashMap<String, String>();
			switch(ConfLoader.getInstance().getSrcObjectType()) {
			case TABLE:
				objTypes = new String[] {"TABLE"};
				try (ResultSet tables = metaData.getTables(catalog, schema, objectNamePattern, objTypes)) {
					while (tables.next()) {
						String tableName = tables.getString("TABLE_NAME");
						objectMap.put(tableName, "TABLE");
					}
				}
				break;
			case VIEW:
				objTypes = new String[] {"VIEW"};
				try (ResultSet tables = metaData.getTables(catalog, schema, objectNamePattern, objTypes)) {
					while (tables.next()) {
						String tableName = tables.getString("TABLE_NAME");
						objectMap.put(tableName, "VIEW");
					}
				}
				break;
			case ALL :
				objTypes = new String[] {"TABLE"};
				try (ResultSet tables = metaData.getTables(catalog, schema, objectNamePattern, objTypes)) {
					while (tables.next()) {
						String tableName = tables.getString("TABLE_NAME");
						objectMap.put(tableName, "TABLE");
					}
				}
				objTypes = new String[] {"VIEW"};
				try (ResultSet tables = metaData.getTables(catalog, schema, objectNamePattern, objTypes)) {
					while (tables.next()) {
						String tableName = tables.getString("TABLE_NAME");
						objectMap.put(tableName, "VIEW");
					}
				}
			}
			return objectMap;
		} catch (Exception e) {
			throw new SyncLiteException("Failed to read object info from source database : " + e.getMessage(), e);
		}
	}

	private static HashMap<String, String> loadObjectNamesFromMetadataCSV() throws SyncLiteException {
		//Get all folder names in the given path.
		HashMap<String, String> objectMap= new HashMap<String, String>();

		downloadObjectsInLocalFSDir();
		Path dataDirPath = ConfLoader.getInstance().getSrcFileStorageLocalFSDirectory();

		try (Connection c = DriverManager.getConnection("jdbc:sqlite::memory:"); 
				Statement s = c.createStatement();
				DirectoryStream<Path> stream = Files.newDirectoryStream(dataDirPath)) {
			// Iterate over the directory contents
			for (Path entry : stream) {
				// Check if the entry is a directory
				if (Files.isDirectory(entry)) {
					//Check objectName pattern
					String objName = entry.getFileName().toString();
					try (ResultSet rs = s.executeQuery("SELECT 1 WHERE '" + objName + "' LIKE '" + ConfLoader.getInstance().getSrcObjectNamePattern() + "'")) {
						if (rs.next()) {
							objectMap.put(objName, "TABLE");
						}
					}
				}
			}
		} catch (Exception e) {
			throw new SyncLiteException("Failed to read contents of specified data directory path : " + e.getMessage(), e);
		}
		return objectMap;
	}


	private static final void downloadObjectsInLocalFSDir() throws SyncLiteException {
		FileStorageType storageType = ConfLoader.getInstance().getSrcFileStorageType();
		switch(storageType) {
		case SFTP:
			//List all directories under specified remote directory path.
			downloadObjectsFromSFTPToLocalFSDir();
			break;
		case S3:
			//List all directories under specified remote directory path.
			downloadObjectsFromS3ToLocalFSDir();
			break;
		}		
	}


	private static final void downloadObjectsFromSFTPToLocalFSDir() throws SyncLiteException {
		try {			
			String user = ConfLoader.getInstance().getSrcFileStorageSFTPUser();
			String password = ConfLoader.getInstance().getSrcFileStorageSFTPPassword();
			String host = ConfLoader.getInstance().getSrcFileStorageSFTPHost();
			Integer port = ConfLoader.getInstance().getSrcFileStorageSFTPPort();
			String remoteDirectory = ConfLoader.getInstance().getSrcFileStorageSFTPDirectory();

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

			Path localDirPath = ConfLoader.getInstance().getSrcFileStorageLocalFSDirectory();
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
			throw new SyncLiteException("Failed to list objects from the specified SFTP location : " + e.getMessage(), e);
		}
	}

	private static final void downloadObjectsFromS3ToLocalFSDir() throws  SyncLiteException {
		try {			
			String url = ConfLoader.getInstance().getSrcFileStorageS3Url();
			String bucketName = ConfLoader.getInstance().getSrcFileStorageS3BucketName();
			String accessKey = ConfLoader.getInstance().getSrcFileStorageS3AccessKey();
			String secretKey = ConfLoader.getInstance().getSrcFileStorageS3SecretKey();

			AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

			AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(credentials))
					.withEndpointConfiguration(new EndpointConfiguration(url, AwsHostNameUtils.parseRegion(url, null)))
					.build();


			Path localDirPath = ConfLoader.getInstance().getSrcFileStorageLocalFSDirectory();

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
			throw new SyncLiteException("Failed to list objects from the specified S3 location : " + e.getMessage(), e);
		}
	}


}
