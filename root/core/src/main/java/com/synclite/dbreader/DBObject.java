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

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONTokener;

import io.synclite.logger.*;

public class DBObject {
	private static final int MAX_SYNCLITE_DEVICE_NAME_LENGTH = 64;
	private String name;
	private int position;
	private String fullName;
	private String keyTableName;
	private String keyTableFullName;
	private String deviceName;
	private List<String> columnsUpperCase;
	private int partitionIdx;
	private HashMap<Integer, String> selectConditions;
	private String selectConditionsJson;
	private HashMap<String, String> columnDataTypes;
	private HashMap<String, String> columnDefMap;
	private String columnDefJson;
	private String columnDefStr;
	private String columnsStr;
	private String uniqueKeyColumnsStr;
	private String quotedUniqueKeyColumnsStr;
	private List<String> uniqueKeyColumns;
	private List<String> uniqueKeyColumnsUpperCase;
	private String maskColumnsStr;
	private HashSet<String> maskColumns;	
	private String incrementalKeyColumnsStr;
	private List<String> incrementalKeyColumns;
	private List<String> incrementalKeyColumnsUpperCase;
	private HashMap<String, IncrementalKeyType> incrementalKeyTypes;
	private String deleteCondition;
	private Path deviceFilePath;
	private String dropTableSql;
	private String createTableSql;
	private String refreshTableSql;
	private String insertTableSql;
	private String updateTableSql;
	private String deleteTableSql;
	private String selectTableSql;
	private String createKeyTableSql;
	private String dropKeyTableSql;
	private String insertKeyTableSql;
	private String selectKeyTableSql;
	private String softDeleteBasedDeleteTableSql;
	private String keyBasedDeleteTableSql;
	private String maxSql;
	private HashMap<String, String> lastReadIncrementalKeyColVals = new HashMap<String, String>();
	private String lastReadLogPosition = "";
	private Long lastReadLogTS = 0L;
	private boolean loggedRefreshSchemas;
	private Logger tracer;
	private boolean reloadSchemaOnNextRestart = false;
	private boolean reloadSchemaOnEachRestart = false;
	private boolean reloadObjectOnNextRestart = false;
	private boolean reloadObjectOnEachRestart = false;

	//Statistics
	private volatile long lastReadStartTime;
	private volatile long lastReadEndTime;
	private volatile AtomicLong lastReadRecordsFetched = new AtomicLong(0);
	private volatile long lastDeleteSyncStartTime;
	private volatile long lastDeleteSyncEndTime;
	private volatile AtomicLong lastDeleteSyncKeysFetched = new AtomicLong(0);
	private volatile String lastReadStatus = "";
	private volatile String lastReadStatusDescription = "";
	private volatile AtomicLong totalRecordsFetched = new AtomicLong(0);
	private volatile AtomicLong totalInferredDDLs = new AtomicLong(0);
	private volatile AtomicLong totalFullReloadCount = new AtomicLong(0);
	private volatile AtomicLong totalSchemaReloadCount = new AtomicLong(0);
	private volatile long lastPublishedCommitID = 0;

	public String getReadType() {
		if (ConfLoader.getInstance().getSrcDBReaderMethod() == DBReaderMethod.INCREMENTAL) {
			if (isIncrementalKeyConfigured()) {
				return "INCREMENTAL";
			}
		}
		else if (ConfLoader.getInstance().getSrcDBReaderMethod() == DBReaderMethod.LOG_BASED) {
			return "SNAPSHOT + LOG_BASED";
		}
		return "SNAPSHOT";
	}

	public long getLastReadStartTime() {
		return this.lastReadStartTime;
	}

	public long getLastPublishedCommitID() {
		return this.lastPublishedCommitID;
	}

	public void setLastPublishedCommitID(long val) {
		this.lastPublishedCommitID = val;
	}

	public void setLastReadStartTime(long t) {
		this.lastReadStartTime = t;
	}

	public long getLastReadEndTime() {
		return this.lastReadEndTime;
	}

	public void setLastReadEndTime(long t) {
		this.lastReadEndTime = t;
	}

	public long getLastReadRecordsFetched() {
		return this.lastReadRecordsFetched.longValue();
	}

	public void setLastReadRecordsFetched(long cnt) {
		this.lastReadRecordsFetched.set(cnt);
	}

	public void incrLastReadRecordsFetched(long delta) {
		this.lastReadRecordsFetched.addAndGet(delta);
		this.totalRecordsFetched.addAndGet(delta);
		Monitor.getInstance().incrTotalRecordsFetched(delta);
	}

	public String getLastReadStatus() {
		return this.lastReadStatus;
	}

	public void setLastReadStatus(String status) {
		this.lastReadStatus = status;
	}

	public String getLastReadStatusDescription() {
		return this.lastReadStatusDescription;
	}

	public void setLastReadStatusDescription(String descr) {
		this.lastReadStatusDescription = descr;
	}

	public long getTotalRecordsFetched() {
		return this.totalRecordsFetched.longValue();
	}

	public void setTotalRecordsFetched(long cnt) {
		this.totalRecordsFetched.set(cnt);
	}

	public long getLastDeleteSyncStartTime() {
		return this.lastDeleteSyncStartTime;
	}

	public void setLastDeleteSyncStartTime(long t) {
		this.lastDeleteSyncStartTime = t;
	}

	public long getLastDeleteSyncEndTime() {
		return this.lastDeleteSyncEndTime;
	}

	public void setLastDeleteSyncEndTime(long t) {
		this.lastDeleteSyncEndTime = t;
	}

	public long getLastDeleteSyncKeysFetched() {
		return this.lastDeleteSyncKeysFetched.longValue();
	}

	public void setLastDeleteSyncKeysFetched(long cnt) {
		this.lastDeleteSyncKeysFetched.set(0);
	}

	public void incrLastDeleteSyncKeysFetched(long delta) {
		this.lastDeleteSyncKeysFetched.addAndGet(delta);
	}

	public String getLastReadLogPosition() {
		return this.lastReadLogPosition;
	}

	public long getLastReadLogTS() {
		return this.lastReadLogTS;
	}

	public String getLastReadIncrementalKeyColValues() {
		StringBuilder valList = new StringBuilder();
		boolean first = false;
		for (Map.Entry<String, String> entry : this.lastReadIncrementalKeyColVals.entrySet()) {
			if (first) {
				valList.append(",");
			}
			valList.append(entry.getKey());
			valList.append("(");
			valList.append(entry.getValue());
			valList.append(")");
		}
		return valList.toString();
	}

	public long getTotalInferredDDLs() {
		return this.totalInferredDDLs.longValue();
	}

	public void setTotalInferredDDLs(long cnt) {
		this.totalInferredDDLs.set(cnt);
	}	

	public long incrInferredDDLCount(long delta) {
		return this.totalInferredDDLs.addAndGet(delta);
	}

	public long getTotalFullReloadCount() {
		return this.totalFullReloadCount.longValue();
	}

	public void setTotalFullReloadCount(long cnt) {
		this.totalFullReloadCount.set(cnt);
	}

	public void incrFullReloadCount(long delta) {
		this.totalFullReloadCount.addAndGet(delta);
	}

	public long getTotalSchemaReloadCount() {
		return this.totalSchemaReloadCount.longValue();
	}

	public void setTotalSchemaReloadCount(long cnt) {
		this.totalSchemaReloadCount.set(cnt);
	}

	public void incrSchemaReloadCount(long delta) {
		this.totalSchemaReloadCount.addAndGet(delta);
	}

	public DBObject(Logger tracer, String name, String columnDefJson, String uniqueKeyColumnsStr, String incrementalKeyColumnStr, String group, int position, String maskColumnsStr, String deleteCondition, int partitionIdx, String selectConditionsJson, HashMap<Integer, String> selectConditions) throws SyncLiteException {
		this.tracer = tracer;
		initializeDBObject(name, columnDefJson, uniqueKeyColumnsStr, incrementalKeyColumnStr, group, position, maskColumnsStr, deleteCondition, partitionIdx, selectConditionsJson, selectConditions);
	}

	public void initializeDBObject(String name, String columnDefJson, String uniqueKeyColumnsStr, String incrementalKeyColumnStr, String group, int position, String maskColumnsStr, String deleteCondition, int partitionIdx, String selectConditionsJson, HashMap<Integer, String> selectConditions) throws SyncLiteException {
		this.name = name;
		if ((group == null) || (group.isBlank())) {
			this.deviceName = name.replaceAll("[^a-zA-Z0-9]", "");
			if (this.deviceName.length() > 64) {
				if (this.partitionIdx > 0) {
					int lenPartIdx = String.valueOf(partitionIdx).length();
					this.deviceName = this.deviceName.substring(0, MAX_SYNCLITE_DEVICE_NAME_LENGTH - lenPartIdx) + this.partitionIdx;
				} else {
					this.deviceName = this.deviceName.substring(0, MAX_SYNCLITE_DEVICE_NAME_LENGTH);
				}
			}
		} else {
			this.deviceName = group;
		}
		this.position = position;
		this.fullName = getFullSourceObjectName(name);
		this.keyTableName = name + "_keys";
		this.keyTableFullName = getFullSourceObjectName(this.keyTableName);
		this.columnDefJson = columnDefJson;
		this.partitionIdx = partitionIdx;
		this.selectConditionsJson = selectConditionsJson;
		this.selectConditions = selectConditions;
		//Below order of validations is important. Do not reorder.
		validateAndParseAllowedColList(columnDefJson);
		validateAndParseUniqueKeyColList(uniqueKeyColumnsStr);
		validateAndParseIncrementalKeyColList(incrementalKeyColumnStr);
		validateAndParseMaskColList(maskColumnsStr);
		validateAndParseDeleteCondition(deleteCondition);
		populateSqls();
	}

	public boolean getLoggedRereshSchemas() {
		return this.loggedRefreshSchemas;
	}

	public void setLoggedRereshSchemas() {
		this.loggedRefreshSchemas = true;
	}

	public final void configureSyncLiteDevice() throws SyncLiteException {
		try {
			this.deviceFilePath = ConfLoader.getInstance().getSyncLiteDeviceDir().resolve(this.deviceName + ".db");
			Class.forName("io.synclite.logger.Telemetry");
			Telemetry.initialize(this.deviceFilePath, ConfLoader.getInstance().getSyncLiteLoggerConfigurationFile(), this.deviceName);
			String deviceURL = "jdbc:synclite_telemetry:" + this.deviceFilePath;
			try (Connection conn = DriverManager.getConnection(deviceURL)) {
				conn.setAutoCommit(false);
				try (TelemetryStatement stmt = (TelemetryStatement) conn.createStatement()) {
					stmt.executeUnlogged("CREATE TABLE IF NOT EXISTS synclite_dbreader_checkpoint(object_name TEXT, column_name TEXT, column_type TEXT, column_value TEXT)");
					if (this.doReloadObject()) {
						this.tracer.info("Reload object requested for : " + this.fullName + ", resetting checkpoint table");
						//If user has asked to reload objects then drop synclite_dbreader_checkpoint so that full object reload will be done
						stmt.executeUnlogged("DELETE FROM synclite_dbreader_checkpoint WHERE object_name = '" + this.name + "'");
						//
						//Dont publish DROP TABLE SQL as that should be controlled by the dst-object-init-mode option in Consolidation.
						//stmt.execute(dropTableSql);
					}
					int incrKeyCount = 0;
					boolean incrKeyChanged = false;
					try(ResultSet rs = stmt.executeQuery("SELECT count(*) FROM synclite_dbreader_checkpoint WHERE object_name = '" + this.name + "'")) {
						incrKeyCount = rs.getInt(1);
						if (this.incrementalKeyColumns.size() != incrKeyCount) {
							incrKeyChanged= true;
						}
					}

					if (incrKeyCount > 0) {
						if (incrKeyChanged == false) {
							//Check if all keys are as configured or configured incremental keys have changed.
							try(ResultSet rs = stmt.executeQuery("SELECT column_name, column_type, column_value FROM synclite_dbreader_checkpoint WHERE object_name = '" + this.name + "'")) {
								while (rs.next()) {
									String checkpointedIncrementalKeyCol = rs.getString("column_name");
									String checkpointedIncrementalKeyColVal = rs.getString("column_value");
									if (!incrementalKeyColumns.contains(checkpointedIncrementalKeyCol)) {
										incrKeyChanged = true;
										break;
									}
									this.lastReadIncrementalKeyColVals.put(checkpointedIncrementalKeyCol, checkpointedIncrementalKeyColVal);
								}
							}
						}

						if (incrKeyChanged) {
							this.tracer.info("Incremental key configuration changed for object : " + this.fullName + ", resetting checkpoint table");							
							//Delete and reinsert fresh incremental key column with initial values.
							stmt.executeUnlogged("DELETE FROM synclite_dbreader_checkpoint WHERE object_name = '" + this.name + "'");
							for (String incrKey : this.incrementalKeyColumns) {
								String colName = incrKey;
								String colType = getColumnDataType(incrKey);
								String colVal = getInitialIncrKeyColVal(incrKey);								
								stmt.executeUnlogged("INSERT INTO synclite_dbreader_checkpoint(object_name, column_name, column_type, column_value) VALUES('" + this.name + "', '" + colName + "', '" + colType + "', '" + colVal + "')");							
								this.lastReadIncrementalKeyColVals.put(colName, colVal);
							}
						}
					} else {
						//We are starting up for this object.
						//Just insert fresh entries
						//if no incremental key configured, insert one entry for checkpointing
						//
						for (String incrKey : this.incrementalKeyColumns) {
							String colName = incrKey;
							String colType = getColumnDataType(incrKey);
							String colVal = getInitialIncrKeyColVal(incrKey);
							stmt.executeUnlogged("INSERT INTO synclite_dbreader_checkpoint(object_name, column_name, column_type, column_value) VALUES('" + this.name + "', '" + colName + "', '" + colType + "', '" + colVal + "')");							
							this.lastReadIncrementalKeyColVals.put(colName, colVal);							 
						}	
						stmt.execute(createTableSql);
					}
					if (ConfLoader.getInstance().getSrcDBReaderMethod() == DBReaderMethod.LOG_BASED) {
						stmt.executeUnlogged("CREATE TABLE IF NOT EXISTS synclite_logreader_checkpoint(object_name TEXT, log_position TEXT, log_ts LONG)");
						try(ResultSet rs = stmt.executeQuery("SELECT log_position, log_ts FROM synclite_logreader_checkpoint WHERE object_name = '" + this.name + "'")) {
							if (rs.next()) {
								this.lastReadLogPosition = rs.getString("log_position");
								this.lastReadLogTS = rs.getLong("log_ts");
							} else {
								stmt.executeUnlogged("INSERT INTO synclite_logreader_checkpoint(object_name, log_position, log_ts) VALUES('" + this.name + "', '', '" + System.currentTimeMillis()  + "')");							
							}
						}
					}
				}
				conn.commit();
			}
		} catch (Exception e) {
			this.tracer.error("Failed to configure SyncLite device for object : " + this.fullName + " : " + e.getMessage(), e);
			throw new SyncLiteException("Failed to configure SyncLite device for object : " + this.fullName + " : " + fullName, e);
		}
	}

	public String getInitialIncrKeyColVal(String incrKey) {
		IncrementalKeyType type = this.incrementalKeyTypes.get(incrKey.toUpperCase());
		switch(type) {
		case TIMESTAMP:
			String val = ConfLoader.getInstance().getSrcTimestampIncrementalKeyInitialValue();
			if ((ConfLoader.getInstance().getSrcType() == SrcType.CSV) || (ConfLoader.getInstance().getSrcType() == SrcType.MONGODB)) {
				val = val.replaceFirst(" ", "T") + "Z";
			}
			return val;			
		case NUMERIC:
			return ConfLoader.getInstance().getSrcNumericIncrementalKeyInitialValue();
		}
		return "0";
	}

	public String getColumnDataType(String colName) {
		return this.columnDataTypes.get(colName.toUpperCase());
	}
	
	public boolean isIncrementalKeyConfigured() {
		if (this.incrementalKeyColumnsStr != null) {
			if (!this.incrementalKeyColumnsStr.isBlank()) {
				return true;
			}
		}
		return false;
	}

	private String getFullSourceObjectName(String name) {
		SrcType srcType = ConfLoader.getInstance().getSrcType();
		String database = ConfLoader.getInstance().getSrcDatabase();
		String schema = ConfLoader.getInstance().getSrcSchema();
		boolean prefixCatalog = ConfLoader.getInstance().getSrcUseCatalogScopeResolution();
		boolean prefixSchema = ConfLoader.getInstance().getSrcUseSchemaScopeResolution();
		
		String tableName = "";
		if (database != null) {
			if (schema != null) {
				if (prefixCatalog) {
					if (prefixSchema) {
						tableName = quoteObjectNameIfNeeded(database) + "." + quoteObjectNameIfNeeded(schema) + "." + quoteObjectNameIfNeeded(name);
					} else {
						tableName = quoteObjectNameIfNeeded(database) + "." + quoteObjectNameIfNeeded(name);
					}
				} else {
					if (prefixSchema) {
						tableName = quoteObjectNameIfNeeded(schema) + "." + quoteObjectNameIfNeeded(name);
					} else {
						tableName = quoteObjectNameIfNeeded(name);
					}				
				}
			} else {
				if (prefixCatalog) {
					tableName = quoteObjectNameIfNeeded(database) + "." +  quoteObjectNameIfNeeded(name);
				} else {
					tableName = quoteObjectNameIfNeeded(name);
				}
			}
		} else {
			if (schema != null) {
				if (prefixSchema) {
					tableName = quoteObjectNameIfNeeded(schema) + "." + quoteObjectNameIfNeeded(name);
				} else {
					tableName = quoteObjectNameIfNeeded(name);
				}
			} else {
				tableName = quoteObjectNameIfNeeded(name);
			}			
		}
		return tableName;
	}

	private final String quoteObjectNameIfNeeded(String item) {
		if (ConfLoader.getInstance().getSrcQuoteObjectNames()) {
			return quote(item);
		}
		return item;
	}

	private void populateSqls() {
		createTableSql = "CREATE TABLE IF NOT EXISTS " + name + "(" + columnDefStr;
		if (!quotedUniqueKeyColumnsStr.isBlank()) {
			createTableSql += ", PRIMARY KEY(" + quotedUniqueKeyColumnsStr + ")";
		}
		createTableSql += ")";

		dropTableSql = "DROP TABLE IF EXISTS " + name;
		refreshTableSql = "REFRESH TABLE " + name + "(" + columnDefStr;
		if (!uniqueKeyColumnsStr.isBlank()) {
			refreshTableSql += ", PRIMARY KEY(" + quotedUniqueKeyColumnsStr + ")";
		}
		refreshTableSql += ")";
		insertTableSql = "INSERT INTO " + name + " VALUES(";
		updateTableSql = "UPDATE " + name + " SET ";
		deleteTableSql = "DELETE FROM " + name + " WHERE ";		
		boolean first = true;
		StringBuilder insertTableSqlBuilder = new StringBuilder();
		StringBuilder updateTableSetSqlBuilder = new StringBuilder();
		StringBuilder updateDeleteTableWereSqlBuilder = new StringBuilder();
		for (int i =0; i < columnsUpperCase.size(); ++i) {
			if (!first) {
				insertTableSqlBuilder.append(",");
				updateTableSetSqlBuilder.append(",");
				updateDeleteTableWereSqlBuilder.append(" AND ");
			}
			insertTableSqlBuilder.append("?");
			updateTableSetSqlBuilder.append(columnsUpperCase.get(i) + " = ?");
			updateDeleteTableWereSqlBuilder.append(columnsUpperCase.get(i) + " = ?");
			first = false;
		}
		insertTableSql += insertTableSqlBuilder.toString() + ")";
		updateTableSql += updateTableSetSqlBuilder.toString() + " WHERE " + updateDeleteTableWereSqlBuilder;
		deleteTableSql += updateDeleteTableWereSqlBuilder.toString();

		boolean selectContainsWhere = false;
		selectTableSql = "SELECT " + columnsStr + " FROM " + fullName;

		if (isIncrementalKeyConfigured()) {			
			//Iterate on all incremental keys and generate OR conditions
			//
			first = true;
			StringBuilder incrCondBuilder = new StringBuilder();
			StringBuilder maxSqlSelectListBuilder = new StringBuilder();

			if (ConfLoader.getInstance().getSrcComputeMaxIncrementalKeyInDB()) {
				int idx = 1;
				for (String colName : this.incrementalKeyColumns) {
					if (first) {
						incrCondBuilder.append(" WHERE (");
						maxSqlSelectListBuilder.append("MAX(" + quoteColumnName(colName) + ")");
					} else {
						incrCondBuilder.append(" OR ");
						maxSqlSelectListBuilder.append(", MAX(" + quoteColumnName(colName) + ")");
					}
					incrCondBuilder.append("(");
	
					incrCondBuilder.append(quoteColumnName(colName));
					incrCondBuilder.append(" > ");
					String expr1 = "";
					String expr2 = "";
					if (this.incrementalKeyTypes.get(colName.toUpperCase()) == IncrementalKeyType.TIMESTAMP) {
						String srcQueryTimestampConversionFunction = ConfLoader.getInstance().getSrcQueryTimestampConversionFunction();
						if ((srcQueryTimestampConversionFunction != null) && (!srcQueryTimestampConversionFunction.isBlank())) {
							expr1 = srcQueryTimestampConversionFunction.replace("$", "$" + String.valueOf(idx));
							expr2 = srcQueryTimestampConversionFunction.replace("$", "$" + String.valueOf(idx + 1));
						} else {
							expr1 = "'$" + idx + "'";
							expr2 = "'$" + String.valueOf(idx + 1) + "'";
						}
					}
					incrCondBuilder.append(expr1);
					incrCondBuilder.append(" AND ");
					incrCondBuilder.append(quoteColumnName(colName));
					incrCondBuilder.append(" <= ");
					incrCondBuilder.append(expr2);
					incrCondBuilder.append(")");
					if (ConfLoader.getInstance().getSrcReadNullIncrementalKeyRecords()) {
						incrCondBuilder.append(" OR (");
						incrCondBuilder.append(quoteColumnName(colName));
						incrCondBuilder.append(" IS NULL");
						incrCondBuilder.append(") "); 
					}
					first = false;
					idx = idx + 2;
				}
				incrCondBuilder.append(")");
				maxSql = "SELECT " + maxSqlSelectListBuilder.toString() + " FROM " + fullName;
			} else {
				int idx = 1;
				for (String colName : this.incrementalKeyColumns) {
					if (first) {
						incrCondBuilder.append(" WHERE (");
					} else {
						incrCondBuilder.append(" OR ");
					}
					incrCondBuilder.append("(");
	
					incrCondBuilder.append(quoteColumnName(colName));
					incrCondBuilder.append(" > ");
					String expr1 = "";
					if (this.incrementalKeyTypes.get(colName.toUpperCase()) == IncrementalKeyType.TIMESTAMP) {
						String srcQueryTimestampConversionFunction = ConfLoader.getInstance().getSrcQueryTimestampConversionFunction();
						if ((srcQueryTimestampConversionFunction != null) && (!srcQueryTimestampConversionFunction.isBlank())) {
							expr1 = srcQueryTimestampConversionFunction.replace("$", "$" + String.valueOf(idx));
						} else {
							expr1 = "'$" + idx + "'";
						}
					}
					incrCondBuilder.append(expr1);
					incrCondBuilder.append(")");
					if (ConfLoader.getInstance().getSrcReadNullIncrementalKeyRecords()) {
						incrCondBuilder.append(" OR (");
						incrCondBuilder.append(quoteColumnName(colName));
						incrCondBuilder.append(" IS NULL");
						incrCondBuilder.append(") "); 
					}
					first = false;
					++idx;
				}
				incrCondBuilder.append(")");
				maxSql = "SELECT 1";  //NoOp
			}

			selectTableSql += incrCondBuilder.toString();


			if (this.partitionIdx > 0) {
				//User has supplied conditions
				if ((this.selectConditions != null) && (this.selectConditions.get(partitionIdx) != null) && (!this.selectConditions.get(partitionIdx).isBlank())) {
					selectTableSql += " AND (" + this.selectConditions.get(partitionIdx) + ")";
					maxSql += " WHERE " + this.selectConditions.get(partitionIdx);
				}
			}
			selectContainsWhere = true;
		} else {
			if (this.partitionIdx > 0) {
				//User has supplied conditions
				if ((this.selectConditions != null) && (this.selectConditions.get(partitionIdx) != null) && (!this.selectConditions.get(partitionIdx).isBlank())) {
					selectTableSql += " WHERE " + this.selectConditions.get(partitionIdx);
					selectContainsWhere = true;
				}
			}
		}

		if (ConfLoader.getInstance().getSrcDBReaderObjectRecordLimit() > 0) {
			switch(ConfLoader.getInstance().getSrcType()) {
			case MYSQL:
			case POSTGRESQL:
			case SQLITE:
			case DUCKDB:
				selectTableSql += " LIMIT " + ConfLoader.getInstance().getSrcDBReaderObjectRecordLimit();
				break;
			}
		}

		if ((this.deleteCondition != null) && (!this.deleteCondition.isBlank())) {
			softDeleteBasedDeleteTableSql = "DELETE FROM " + name + " WHERE " + this.deleteCondition;
		} else {
			softDeleteBasedDeleteTableSql = null;
		}		

		if ((uniqueKeyColumns != null) && (!uniqueKeyColumns.isEmpty())) {			
			StringBuilder createKeyTableSqlBuilder = new StringBuilder(); 					
			createKeyTableSqlBuilder.append("CREATE TABLE IF NOT EXISTS " + keyTableName + "(");			
			StringBuilder insertKeyTableSqlBuilder = new StringBuilder();
			insertKeyTableSqlBuilder.append("INSERT INTO " +  keyTableName + " VALUES(");
			StringBuilder selectKeyTableSqlBuilder = new StringBuilder();
			selectKeyTableSqlBuilder.append("SELECT ");

			first = true;
			for (String uniqueKeyCol : uniqueKeyColumns) {
				if (!first) {
					createKeyTableSqlBuilder.append(",");
					insertKeyTableSqlBuilder.append(",");
					selectKeyTableSqlBuilder.append(",");
				}
				createKeyTableSqlBuilder.append(quoteColumnName(uniqueKeyCol) + " " + columnDefMap.get(uniqueKeyCol.toUpperCase()));
				insertKeyTableSqlBuilder.append("?");
				selectKeyTableSqlBuilder.append(quoteColumnName(uniqueKeyCol));
				first = false;
			}
			createKeyTableSqlBuilder.append(", PRIMARY KEY(" + quotedUniqueKeyColumnsStr + ")");
			createKeyTableSqlBuilder.append(")");
			insertKeyTableSqlBuilder.append(")");
			selectKeyTableSqlBuilder.append(" FROM " + fullName);

			this.createKeyTableSql = createKeyTableSqlBuilder.toString();
			this.insertKeyTableSql = insertKeyTableSqlBuilder.toString();
			this.selectKeyTableSql = selectKeyTableSqlBuilder.toString();
			this.dropKeyTableSql = "DROP TABLE IF EXISTS " + keyTableName;
			keyBasedDeleteTableSql = "MINUS " + name + " " + keyTableName;

			if ((this.selectConditions != null) && (this.selectConditions.size() > 0)) {
				//
				//Append a disjunction of all conditions to the select query so that we only read PK values for
				//the records being replicated.
				//
				StringBuilder whereClauseBuilder = new StringBuilder();
				whereClauseBuilder.append(" WHERE ");
				first = true;
				for (Map.Entry<Integer, String> entry : this.selectConditions.entrySet()) {
					String cond = entry.getValue();
					if ((cond != null) && (!cond.isBlank())) {
						if (!first) {
							whereClauseBuilder.append(" OR ");
						}
						whereClauseBuilder.append("(");
						whereClauseBuilder.append(cond);
						whereClauseBuilder.append(")");
						first = false;
					}
				}

				if (first == false) {
					this.selectKeyTableSql += whereClauseBuilder.toString();				
				}					
			}
		}
	}

	private void validateAndParseIncrementalKeyColList(String incrKeyColumnsStr) throws SyncLiteException {
		if (incrKeyColumnsStr != null) {
			this.incrementalKeyColumnsStr = incrKeyColumnsStr.strip();
		} else {
			this.incrementalKeyColumnsStr = "";
		}
		this.incrementalKeyColumns = new ArrayList<String>();
		this.incrementalKeyColumnsUpperCase = new ArrayList<String>();
		this.incrementalKeyTypes = new HashMap<String, IncrementalKeyType>();

		if (!this.incrementalKeyColumnsStr.isBlank()) {
			String[] tokens = this.incrementalKeyColumnsStr.split(",");
			for (int j=0; j < tokens.length; ++j) {
				String colName = tokens[j].strip().toUpperCase();
				if (ConfLoader.getInstance().getSrcType() == SrcType.MONGODB) {
					//
					//No validation for mongodb as fields are dynamic
					//
					incrementalKeyColumns.add(tokens[j].strip());
					incrementalKeyColumnsUpperCase.add(colName);
					this.incrementalKeyTypes.put(colName, IncrementalKeyType.TIMESTAMP);
				} else { 
					if (! columnsUpperCase.contains(colName)) {
						throw new SyncLiteException("Specified column in incremental key columns : " + colName + " is not present in allowed columns for object : " + this.fullName);
					}
					//Add original text ( not the upper case converted)
					incrementalKeyColumns.add(tokens[j].strip());
					incrementalKeyColumnsUpperCase.add(colName);
					String incrementalKeyColumnDataType = columnDataTypes.get(colName);
					try {
						IncrementalKeyType type = validateIncrementalKeyColumnDataType(colName, incrementalKeyColumnDataType);
						this.incrementalKeyTypes.put(colName, type);
					} catch (SyncLiteException e) {
						throw new SyncLiteException("Data type for specified incremental key column : " + colName + " for object : " + fullName + " is " + incrementalKeyColumnDataType + ". It must be a datetime/timestamp/numeric data type");
					}
				}
			}			
			this.tracer.info("Using specified object specific incremental key columns for object : " + this.fullName + ", incremental keys : " + this.incrementalKeyColumnsStr);
		} else if (ConfLoader.getInstance().getSrcDefaultIncrementalKeyColumnList() != null) {
			//Check if default incremental keys are present in the object
			//
			if (ConfLoader.getInstance().getSrcType() == SrcType.CSV) {
				//
				//For CSV source the default incremental key is always FILE_CREATION_TIME if specified.
				//
				this.incrementalKeyColumnsStr = ConfLoader.getInstance().getSrcDefaultIncrementalKeyColumnList().strip();
				this.incrementalKeyColumns.add(this.incrementalKeyColumnsStr);
				this.incrementalKeyColumnsUpperCase.add(this.incrementalKeyColumnsStr);
				this.incrementalKeyTypes.put(this.incrementalKeyColumnsStr, IncrementalKeyType.TIMESTAMP);
				this.tracer.info("Using default incremental key column for object : " + this.fullName + ", incremental keys : " + this.incrementalKeyColumnsStr);
				return;
			}

			this.incrementalKeyColumnsStr = ConfLoader.getInstance().getSrcDefaultIncrementalKeyColumnList().strip();
			String[] tokens = this.incrementalKeyColumnsStr.split(",");
			List<String> tempIncrementalKeyCols = new ArrayList<String>();
			List<String> tempIncrementalKeyColsUpperCase = new ArrayList<String>();
			HashMap<String, IncrementalKeyType> tempIncrementalKeyTypes = new HashMap<String, IncrementalKeyType>();

			for (int j=0; j < tokens.length; ++j) {
				String colName = tokens[j].strip().toUpperCase();
				if (ConfLoader.getInstance().getSrcType() == SrcType.MONGODB) {
					//
					//No validation for mongodb as fields are dynamic
					//
					tempIncrementalKeyCols.add(tokens[j].strip());
					tempIncrementalKeyColsUpperCase.add(colName);
					tempIncrementalKeyTypes.put(colName, IncrementalKeyType.TIMESTAMP);
				} else { 
					if (! columnsUpperCase.contains(colName)) {
						this.tracer.info("Not considering specified default incremental key : " + this.incrementalKeyColumnsStr + " for object : " + this.fullName + " as one of the columns in it : " + colName + " is not present in the object's allowed columns");
						tempIncrementalKeyCols.clear();
						tempIncrementalKeyTypes.clear();
						this.incrementalKeyColumnsStr = "";
						break;
					} else {
						//Add original name (not upper case converted
						tempIncrementalKeyCols.add(tokens[j].strip());
						tempIncrementalKeyColsUpperCase.add(colName);
						String incrementalKeyColumnDataType = columnDataTypes.get(colName);
						try {
							IncrementalKeyType type = validateIncrementalKeyColumnDataType(colName, incrementalKeyColumnDataType);
							tempIncrementalKeyTypes.put(colName, type);
						} catch (SyncLiteException e) {
							this.tracer.info("Not considering specified default incremental key : " + this.incrementalKeyColumnsStr + " for object : " + this.fullName + " as one of the columns in it : " + colName + " is not present in the object's allowed columns");
							tempIncrementalKeyCols.clear();
							tempIncrementalKeyColsUpperCase.clear();
							tempIncrementalKeyTypes.clear();
							this.incrementalKeyColumnsStr = "";
							break;
						}
					}
				}
			}
			if (tempIncrementalKeyCols.size() > 0) {								
				this.incrementalKeyColumns.addAll(tempIncrementalKeyCols);
				this.incrementalKeyColumnsUpperCase.addAll(tempIncrementalKeyColsUpperCase);
				this.incrementalKeyTypes.putAll(tempIncrementalKeyTypes);
				this.tracer.info("Using specified default incremental key columns for object : " + this.fullName + ", incremental keys : " + this.incrementalKeyColumnsStr);
			}
		} else {			
			this.incrementalKeyColumnsStr  = "";	
		}

		//Add a dummy entry in incrementalKeyCols if no incremental replication configured. This entry helps us manage recovery 
		if (this.incrementalKeyColumns.size() == 0) {
			this.incrementalKeyColumns.add("");
			this.incrementalKeyColumnsUpperCase.add("");
			this.incrementalKeyTypes.put("", IncrementalKeyType.NUMERIC);			
		}
	}

	private final void validateAndParseDeleteCondition(String deleteCondition) throws SyncLiteException {
		if (deleteCondition != null) {
			this.deleteCondition = deleteCondition.strip();
		} else {
			this.deleteCondition = "";
		}
		if (! this.deleteCondition.isBlank()) {
			String [] tokens = this.deleteCondition.split("=");
			String deleteKeyColumn = tokens[0].strip().toUpperCase();
			int index = this.columnsUpperCase.indexOf(deleteKeyColumn);
			if (ConfLoader.getInstance().getSrcType() != SrcType.MONGODB) {
				if (index == -1) {
					throw new SyncLiteException("Specified delete condition : " + this.deleteCondition + " appears incorrect as the specified column : "  + deleteKeyColumn + " is not present in allowed columns for object : " + this.fullName);
				}
			}
			this.tracer.info("Using specified object specific soft delete condition for object : " + this.fullName + ", soft delete condition : " + this.deleteCondition);
		} else if (ConfLoader.getInstance().getSrcDefaultSoftDeleteCondition() != null) {
			//Check if default delete condition is specified and see if it is applicable to this object
			this.deleteCondition = ConfLoader.getInstance().getSrcDefaultSoftDeleteCondition().strip();
			String [] tokens = this.deleteCondition.split("=");
			String deleteKeyColumn = tokens[0].strip().toUpperCase();
			int index = this.columnsUpperCase.indexOf(deleteKeyColumn);
			if (ConfLoader.getInstance().getSrcType() != SrcType.MONGODB) {
				if (index == -1) {
					this.tracer.info("Using specified default soft delete condition for object : " + this.fullName + ", soft delete condition : " + this.deleteCondition);
					this.deleteCondition = "";
				} 
			}
		} else {
			this.deleteCondition = "";
		}
	}


	private IncrementalKeyType validateIncrementalKeyColumnDataType(String colName, String dataType) throws SyncLiteException {		
		String dtype = dataType.split("[\\s(]+")[0].toLowerCase();
		switch(dtype) {
		case "date" :
		case "datetime" :
		case "timestamp" :
		case "timestampz":
		case "time":
		case "timez":
		case "smalldatetime":
		case "datetime2":
		case "datetimeoffset":
			return IncrementalKeyType.TIMESTAMP;
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
			return IncrementalKeyType.NUMERIC;
		}
		throw new SyncLiteException("Data type for specified incremental key column : " + colName + " for object : " + fullName + " is " + dataType + ". It must be a datetime/timestamp/numeric data type");
	}

	private void validateAndParseUniqueKeyColList(String uniqueKeyColumnsStr) throws SyncLiteException {
		if (uniqueKeyColumnsStr != null) {
			this.uniqueKeyColumnsStr = uniqueKeyColumnsStr.strip();
		} else {
			this.uniqueKeyColumnsStr = "";
			this.quotedUniqueKeyColumnsStr = "";
		}
		uniqueKeyColumns = new ArrayList<String>();
		uniqueKeyColumnsUpperCase = new ArrayList<String>();
		if (!this.uniqueKeyColumnsStr.isBlank()) {
			StringBuilder quotedUniqueKeyColumnsStrBuilder = new StringBuilder();
			String[] tokens = this.uniqueKeyColumnsStr.split(",");
			for (int j=0; j < tokens.length; ++j) {
				String colName = tokens[j].strip().toUpperCase();
				if (! columnsUpperCase.contains(colName)) {
					throw new SyncLiteException("Specified column in unique key column : " + colName+ " is not present in allowed columns for object : " + this.fullName);
				} else {
					//Add original column name here, no upper case.
					uniqueKeyColumns.add(tokens[j].strip());
					uniqueKeyColumnsUpperCase.add(colName);
					if (j > 0) {
						quotedUniqueKeyColumnsStrBuilder.append(",");
					}
					//Add original column name here, no upper case.
					quotedUniqueKeyColumnsStrBuilder.append(quoteColumnName(tokens[j].strip()));
				}
			}
			this.tracer.info("Using specified object specific unique key columns for object : " + this.fullName + ", unique keys : " + this.uniqueKeyColumnsStr);
			this.quotedUniqueKeyColumnsStr = quotedUniqueKeyColumnsStrBuilder.toString();
		} else if (ConfLoader.getInstance().getSrcDefaultUniqueKeyColumnList() != null) {
			//Check if default unique key is present in the object
			//
			this.uniqueKeyColumnsStr = ConfLoader.getInstance().getSrcDefaultUniqueKeyColumnList().strip();
			String[] tokens = this.uniqueKeyColumnsStr.split(",");
			List<String> tempUniqueKeyCols = new ArrayList<String>();
			List<String> tempUniqueKeyColsUpperCase = new ArrayList<String>();

			StringBuilder quotedUniqueKeyColumnsStrBuilder = new StringBuilder();
			for (int j=0; j < tokens.length; ++j) {
				String colName = tokens[j].strip().toUpperCase();
				if (! columnsUpperCase.contains(colName)) {
					this.tracer.info("Not considering specified default unique key : " + this.uniqueKeyColumnsStr + " for object : " + this.fullName + " as one of the columns in it : " + colName + " is not present in the object's allowed columns");
					tempUniqueKeyCols.clear();
					tempUniqueKeyColsUpperCase.clear();
					this.uniqueKeyColumnsStr = "";
					this.quotedUniqueKeyColumnsStr = "";
					break;
				} else {
					tempUniqueKeyCols.add(tokens[j].strip());
					tempUniqueKeyColsUpperCase.add(colName);
					if (j > 0) {
						quotedUniqueKeyColumnsStrBuilder.append(",");
					}
					//Add original column name here, no upper case.
					quotedUniqueKeyColumnsStrBuilder.append(quoteColumnName(tokens[j].strip()));
				}
			}
			this.uniqueKeyColumns.addAll(tempUniqueKeyCols);
			this.uniqueKeyColumnsUpperCase.addAll(tempUniqueKeyColsUpperCase);
			if (tempUniqueKeyCols.size() > 0) {
				this.tracer.info("Using specified default unique key columns for object : " + this.fullName + ", unique keys : " + this.uniqueKeyColumnsStr);
				this.quotedUniqueKeyColumnsStr = quotedUniqueKeyColumnsStrBuilder.toString();
			}
		} else {
			this.uniqueKeyColumnsStr  = "";
			this.quotedUniqueKeyColumnsStr = "";
		}
	}

	private void validateAndParseMaskColList(String maskColumnsStr) throws SyncLiteException {
		if (maskColumnsStr != null) {
			this.maskColumnsStr = maskColumnsStr.strip();
		} else {
			this.maskColumnsStr = "";
		}
		maskColumns = new HashSet<String>();
		if (!this.maskColumnsStr.isBlank()) {
			String[] tokens = this.maskColumnsStr.split(",");
			for (int j=0; j < tokens.length; ++j) {
				if (ConfLoader.getInstance().getSrcType() == SrcType.MONGODB) {
					//No validation for MongoDB as fields are dynamic
					//
					maskColumns.add(tokens[j].strip());
				} else {
					String colName = tokens[j].strip().toUpperCase();
					if (! columnsUpperCase.contains(colName)) {
						throw new SyncLiteException("Specified column in mask columns : " + colName + " is not present in allowed columns for object : " + this.fullName);
					}				
					if ((uniqueKeyColumnsUpperCase != null) && (uniqueKeyColumnsUpperCase.contains(colName))) {
						throw new SyncLiteException("Specified column in mask columns : " + colName + " is present in primary/unique key columns for object : " + this.fullName + ", masking of primary/unique key columns is not supported");
					}

					if ((incrementalKeyColumnsUpperCase != null) && (incrementalKeyColumnsUpperCase.contains(colName))) {
						throw new SyncLiteException("Specified column in mask columns : " + colName + " is specified as an incremental key column for object : " + this.fullName + ", masking of incremental key column is not supported");
					}				
					//Add original column name, no upper case.
					maskColumns.add(tokens[j].strip());
				}
			}
			this.tracer.info("Using specified object specific mask columns for object : " + this.fullName + ", mask columns : " + this.maskColumnsStr); 
		} else if (ConfLoader.getInstance().getSrcDefaultMaskColumnList() != null) {
			this.maskColumnsStr = ConfLoader.getInstance().getSrcDefaultMaskColumnList().strip();
			String[] tokens = this.maskColumnsStr.split(",");
			for (int j=0; j < tokens.length; ++j) {
				if (ConfLoader.getInstance().getSrcType() == SrcType.MONGODB) {
					//No validation for MongoDB as fields are dynamic.
					//
					maskColumns.add(tokens[j].strip());
				} else {
					String colName = tokens[j].strip().toUpperCase();
					if (columnsUpperCase.contains(colName)) {
						if ((uniqueKeyColumns!= null) && uniqueKeyColumns.contains(colName)) {
							this.tracer.info("Not considering column : " + colName + " as specified in default mask column list for masking since it is part of PK/UK for object : " + this.fullName);
						} else if ((incrementalKeyColumns != null) && (incrementalKeyColumns.contains(colName))) {
							this.tracer.info("Not considering column : " + colName + " as its is specified as default incremental key column for object : " + this.fullName);
						} else {
							//Add original column name, no upper case
							maskColumns.add(tokens[j].strip());
						}
					} 
				}
			}
			if (this.maskColumns.size() > 0) {
				this.tracer.info("Using one or more columns from specified default mask columns for object : " + this.fullName + ", mask columns : " + this.maskColumnsStr); 
			}
		}
	}

	public String getName() {
		return name;
	}

	public String getDeviceName() {
		return deviceName;
	}

	public int getPosition() {
		return position;
	}

	public String getFullName() {
		return fullName;
	}

	public String getKeyTableName() {
		return keyTableName;
	}

	public String getKeyTableFullName() {
		return keyTableFullName;
	}

	public List<String> getColumns() {
		return columnsUpperCase;
	}

	public String getColumnDefStr() {
		return columnDefStr;
	}

	public String getColumnDefJson() {
		return columnDefJson;
	}

	public String getUniqueKeyColumnsStr() {
		return uniqueKeyColumnsStr;
	}

	public String getMaskColumnsStr() {
		return maskColumnsStr;
	}

	public List<String> getUniqueKeyColumns() {
		return uniqueKeyColumns;
	}

	public String getIncrementalKeyColumnStr() {
		return incrementalKeyColumnsStr;
	}

	public List<String> getIncrementalKeyColumns() {
		return incrementalKeyColumns;
	}

	public HashMap<String, IncrementalKeyType> getIncrementalKeyTypes() {
		return incrementalKeyTypes;
	}

	public String getDeleteCondition() {
		return deleteCondition;
	}

	public HashMap<Integer, String> getSelectCoditions() {
		return selectConditions;
	}

	public String getSelectCodition() {
		if ((this.selectConditions != null) && (selectConditions.get(this.partitionIdx) != null) && (!selectConditions.get(partitionIdx).isBlank())) {
			return this.selectConditions.get(partitionIdx);
		}
		return null;
	}
	
	public int getPartitionIdx() {
		return partitionIdx;
	}

	public String getLastReadIncrementalKeyColVal(String colName) {
		return lastReadIncrementalKeyColVals.get(colName);
	}

	public Path getDeviceFilePath() {
		return this.deviceFilePath;
	}

	public String getCreateTableSql() {
		return createTableSql;
	}

	public String getDropTableSql() {
		return dropTableSql;
	}

	public String getRefreshTableSql() {
		return refreshTableSql;
	}

	public String getInsertTableSql() {
		return insertTableSql;
	}

	public String getDeleteTableSql() {
		return deleteTableSql;
	}

	public String getUpdateTableSql() {
		return updateTableSql;
	}

	public String getSoftDeleteBasedDeleteTableSql() {
		return softDeleteBasedDeleteTableSql;
	}

	public String getKeyBasedDeleteTableSql() {
		return keyBasedDeleteTableSql;
	}

	public String getSelectTableSql() {
		return selectTableSql;
	}

	public String getCreateKeyTableSql() {
		return createKeyTableSql;
	}

	public String getInsertKeyTableSql() {
		return insertKeyTableSql;
	}

	public String getSelectKeyTableSql() {
		return selectKeyTableSql;
	}

	public String getDropKeyTableSql() {
		return dropKeyTableSql;
	}

	public String getMaxSql() {
		return maxSql;
	}

	public HashMap<String, String> getColDefMap() {
		return columnDefMap;
	}

	public void setLastReadIncrementalKeyColVal(String col, String val) {
		this.lastReadIncrementalKeyColVals.put(col, val);
	}

	private final void validateAndParseAllowedColList(String allowedColumns) throws SyncLiteException {	
		JSONArray tokens = null;	
		try {
			tokens = new JSONArray(new JSONTokener(allowedColumns));
		} catch (Exception e) {
			throw new SyncLiteException("Specified allowed columns are not in a valid JSON array format for object : " + this.fullName, e);
		}

		StringBuilder columnsStrBuilder = new StringBuilder();
		this.columnsUpperCase = new ArrayList<String>();
		this.columnDataTypes = new HashMap<String, String>();
		this.columnDefMap = new HashMap<String, String>();
		StringBuilder columnDefStrBuilder = new StringBuilder();
		for (int i = 0; i < tokens.length(); ++i) {
			if (i > 0) {
				columnsStrBuilder.append(",");
				columnDefStrBuilder.append(",");
			}
			String[] subTokens = null;
			String tokenToParse = tokens.get(i).toString().strip();
			if (tokenToParse.startsWith("\"")) {
				//column name is quoted , split by quote character.
				subTokens = tokenToParse.substring(1).split("\"", 2);
			} else {
				subTokens = tokenToParse.split("\\s+", 2);
			}
			if (subTokens.length != 2) {
				throw new SyncLiteException("Invalid value : " + tokens.get(i).toString() + " specified for allowed columns for object : " + this.fullName);
			}
			String colName = subTokens[0].strip();
			String colNameUpper = colName.toUpperCase();
			columnsUpperCase.add(colNameUpper);			
			String type = subTokens[1].strip().toLowerCase().split("not null|null", 2)[0].strip();
			columnDataTypes.put(colNameUpper, type);
			columnDefMap.put(colNameUpper, subTokens[1].strip());
			columnsStrBuilder.append(quoteColumnName(colName));
			columnDefStrBuilder.append(tokens.get(i).toString());
		}
		this.columnsStr = columnsStrBuilder.toString();
		this.columnDefStr = columnDefStrBuilder.toString();
	}

	private final static String quote(String t) {
		return "\"" + t + "\"";
	}
	
	public static String quoteColumnName(String colName) {
		if (ConfLoader.getInstance().getSrcQuoteColumnNames()) {
			return quote(colName);
		} else if (colName.matches(".*\\s+.*")) {
			return quote(colName);
		} 
		return colName;
	}

	public boolean hasUniqueKeyDefined() {
		return ((this.uniqueKeyColumns != null) && (!this.uniqueKeyColumns.isEmpty()));
	}

	public boolean hasMaskedColumns() {
		if ((maskColumns != null) && (maskColumns.size() > 0)) {
			return true;
		}
		return false;
	}

	public boolean isColumnMasked(String col) {
		if (maskColumns != null) {
			return maskColumns.contains(col.toUpperCase());
		}
		return false;
	}

	public String getSelectCoditionsJson() {
		return this.selectConditionsJson;
	}

	public boolean doReloadObject() {
		if (ConfLoader.getInstance().getSrcReloadObjects()) {
			return true;
		}

		if (this.reloadObjectOnNextRestart || this.reloadObjectOnEachRestart) {
			return true;
		}
		return false;
	}

	public boolean doReloadSchema() {
		if (ConfLoader.getInstance().getSrcReloadObjectSchemas()) {
			return true;
		}

		if (this.reloadSchemaOnNextRestart || this.reloadSchemaOnEachRestart) {
			return true;
		}
		return false;
	}

	public void setObjectReloadConfigurations(Integer rsnr, Integer rser, Integer ronr, Integer roer) {
		this.reloadSchemaOnNextRestart = (rsnr == 1);
		this.reloadSchemaOnEachRestart = (rser == 1);
		this.reloadObjectOnNextRestart = (ronr == 1);
		this.reloadObjectOnEachRestart = (roer == 1);
	}

	public int getAllowedColumnCount() {
		return this.columnsUpperCase.size();
	}

	public void setLastReadLogPosition(String latestLogPosition) {
		this.lastReadLogPosition = latestLogPosition;
	}

	public void setLastReadLogTS(long latestLogTS) {
		this.lastReadLogTS = latestLogTS;
	}

}
