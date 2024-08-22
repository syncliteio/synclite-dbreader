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
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.bson.Document;

import io.synclite.logger.*;

public class DBReader {

	protected abstract class ValueMasker{
		protected abstract String maskValue(DBObject t, String col, String val);
		protected abstract Document maskDocument(DBObject t, Document d); 
	}

	protected class NoMasker extends ValueMasker{
		protected String maskValue(DBObject t, String col, String val) {
			return val;
		}
		
		protected Document maskDocument(DBObject t, Document d) {
			return d;
		}
	}

	protected class SyncLiteValueMasker extends ValueMasker{
		private String alphaMask = ConfLoader.getInstance().getSrcAlphabeticValueMask();
		private String numMask = ConfLoader.getInstance().getSrcNumericValueMask();

		protected String maskValue(DBObject t, String col, String val) {			
			if (val == null) {
				return null;
			}
			if (t.isColumnMasked(col)) {				
				String ret = val.replaceAll("[a-zA-Z]", alphaMask).replaceAll("[0-9]", numMask);
				return ret;
			}
			return val;
		}
		
		protected Document maskDocument(DBObject t, Document d) {
			Document maskedDocument = new Document();
			for (Entry<String, Object> entry : d.entrySet()) {
				String maskedVal = maskValue(t, entry.getKey(), entry.getValue().toString());
				maskedDocument.append(entry.getKey(), maskedVal);
			}
			return maskedDocument;
		}
	}

	protected final DBObject srcObject;
	protected final Logger tracer;
	protected final ValueMasker valMasker;
	protected DBReader(DBObject object, Logger tracer) {
		this.srcObject = object;
		this.tracer = tracer;		
		if (this.srcObject.hasMaskedColumns()) {
			valMasker = new SyncLiteValueMasker();
		} else {
			valMasker = new NoMasker();
		}
	}
	
	public void processObject() throws SyncLiteException {
		String syncLiteDeviceURL = "jdbc:synclite_telemetry:" + srcObject.getDeviceFilePath();
		boolean reloadingObject = false;
		//Check for refresh schema
		if (srcObject.doReloadSchema() || srcObject.doReloadObject()) {
			if (!srcObject.getLoggedRereshSchemas()) {
				if (srcObject.doReloadSchema()) {
					this.tracer.info("Reload schema requested for object : " + this.srcObject.getFullName());
				} else if (srcObject.doReloadObject()) {
					this.tracer.info("Reload object requested for object : " + this.srcObject.getFullName());
					reloadingObject = true;
				}
				ObjectInfo newObjectInfo = null;
				try {
					//Read fresh schema from source DB and update table object and metadata file.
					newObjectInfo = DBMetadataReader.readSrcSchema(this.srcObject.getName());
				} catch (SyncLiteException e) {
					this.tracer.error("Failed to read object schema from source DB : " + e.getMessage(), e);
					throw new SyncLiteException("Failed to read object schema from source DB : " + e.getMessage(), e);
				}

				if (newObjectInfo != null) {
					try {
						//Refresh table object with latest schema definition.
						srcObject.initializeDBObject(newObjectInfo.name, newObjectInfo.columnDefStr, newObjectInfo.uniqueKeyColumnsStr, srcObject.getIncrementalKeyColumnStr(), srcObject.getDeviceName(), srcObject.getPosition(), srcObject.getMaskColumnsStr(), srcObject.getDeleteCondition(), srcObject.getPartitionIdx(), srcObject.getSelectCoditionsJson(), srcObject.getSelectCoditions());
					} catch(Exception e) {
						this.tracer.error("Failed to initialize object : " + e.getMessage(), e);
						throw new SyncLiteException("Failed to initialize object : " + e.getMessage(), e);
					}

					try {
						//Update latest schema in metadata file
						updateSrcObjectInfoInMetadataTable(srcObject);
					} catch(SyncLiteException e) {
						this.tracer.error("Failed to update object schema in metadata file : " + e.getMessage(), e);
						throw new SyncLiteException("Failed to update object schema in metadata file : " + e.getMessage(), e);				
					}				

					try(Connection deviceConn = DriverManager.getConnection(syncLiteDeviceURL)) {
						try(TelemetryStatement deviceStmt = (TelemetryStatement) deviceConn.createStatement()) {
							deviceStmt.execute(srcObject.getRefreshTableSql());
							srcObject.setLoggedRereshSchemas();
							tracer.info("Logged REFRESH SCHEMA for object : " + srcObject.getFullName());
							//Read last published commitid now.
							try (ResultSet commitIDRS = deviceStmt.executeQuery("SELECT commit_id FROM synclite_txn")) {
								if (commitIDRS.next()) {
									srcObject.setLastPublishedCommitID(commitIDRS.getLong("commit_id"));
								}
							} catch (SQLException e) {
								//Ignore
							}
						}
					} catch (SQLException e) {
						this.tracer.error("Failed to execute refresh object sql for object : " + srcObject.getFullName() + " : " + e.getMessage(), e);
						throw new SyncLiteException("Failed to execute refresh object sql for object : " + srcObject.getFullName() + " : " + e.getMessage(), e);
					}
				}
				this.tracer.info("Reloaded object schema for object : " + this.srcObject.getFullName());
				if (srcObject.doReloadSchema()) {
					srcObject.incrSchemaReloadCount(1);
					Monitor.getInstance().registerChangedObject(srcObject);
				}
			}
		}

		if (!srcObject.isIncrementalKeyConfigured()) {
			//Do it only if this is the first time
			if ((srcObject.getLastReadIncrementalKeyColVal("").equals("0"))) {
				tracer.info("Starting full read on object : " + srcObject.getFullName() + " , partition : " + srcObject.getPartitionIdx());
				//Full object read

				fullRead(reloadingObject);
			} else {
				//Publish operations from LOG if LOG_BASED method is enabled 
				if (ConfLoader.getInstance().getSrcDBReaderMethod() == DBReaderMethod.LOG_BASED) {
					logRead();
				}
			}
		} else {

			if (ConfLoader.getInstance().getSrcInferSchemaChanges() || ConfLoader.getInstance().getSrcInferObjectDrop()) {
				//Check for schema changes
				//1 Read latest schema definition for object : NewMap HashMap<ColumnName, colType NULL/NOT NULL>
				//2 Get existing schema definition map : OldMap
				//3 For entry : NewMap
				//  if entry does not exist in OldMap
				//		Generate ADD COLUMN
				//  else
				//     check definition, if it has changed 
				//		Generate ALTER COLUMN
				//4 For entry : OldMap
				//  if entry does not exist in NewMap
				//		Generate DROP COLUMN
				//
				ObjectInfo newObjectInfo = DBMetadataReader.readSrcSchema(this.srcObject.getName());
				List<String> newDDLs = new ArrayList<String>();
				if (newObjectInfo == null) {
					//Table dropped.
					if (ConfLoader.getInstance().getSrcInferObjectDrop()) {
						String dropTableDDL = "DROP TABLE IF EXISTS " + srcObject.getName();
						newDDLs.add(dropTableDDL);
					}					
					if (!newDDLs.isEmpty()) {
						//Publish DDLs in the device 
						try(Connection deviceConn = DriverManager.getConnection(syncLiteDeviceURL)) {
							try(TelemetryStatement deviceStmt = (TelemetryStatement) deviceConn.createStatement()) {
								for (String ddl : newDDLs) {
									this.tracer.info("Publishing inferred DDL on device : " + ddl );
									deviceStmt.execute(ddl);
								}							

								//Read last published commitid now.
								try (ResultSet commitIDRS = deviceStmt.executeQuery("SELECT commit_id FROM synclite_txn")) {
									if (commitIDRS.next()) {
										srcObject.setLastPublishedCommitID(commitIDRS.getLong("commit_id"));
									}
								} catch (SQLException e) {
									//Ignore
								}
							}
						} catch (SQLException e) {
							//Following check to add imdempotent behavior to ADD COLUMN and DROP COLUMN sqls
							if (e.getMessage().contains("duplicate column name") || e.getMessage().contains("no such column")) {
								//Ignore for idempotency
							} else {
								this.tracer.error("Failed to execute inferred ddl for object " + srcObject.getFullName(), e);
								throw new SyncLiteException("Failed to execute inferred ddl for object : "  + srcObject.getFullName(), e);
							}
						}
					}
					try {
						//delete object entry from metadata file
						//Remove object object from DBReaderDriver and task queue. 
						deleteSrcTableInfoInMetadataFile(srcObject);
						DBReaderDriver.getInstance().removeObject(srcObject);
						this.tracer.info("Object found dropped : " + srcObject.getFullName() + ", aborting replication for this table");

						//Update stats
						srcObject.setLastReadEndTime(System.currentTimeMillis());
						srcObject.setLastReadStatus("SUCCESS");
						srcObject.setLastReadStatusDescription("Object found dropped");
						srcObject.setLastReadRecordsFetched(0);
						srcObject.incrInferredDDLCount(1);
						Monitor.getInstance().registerChangedObject(srcObject);	
						return;
					} catch (Exception e) {
						throw new SyncLiteException("Failed to remove object info from metadata file for object : " + srcObject.getFullName(), e);
					}
				} else {
					if (ConfLoader.getInstance().getSrcInferSchemaChanges()) {
						HashMap<String, String> newColDefMap = newObjectInfo.columnDefMap;
						HashMap<String, String> oldColDefMap =  srcObject.getColDefMap();
						for (Map.Entry<String, String> entry : newColDefMap.entrySet()) {
							String colName = entry.getKey();
							String oldColDef = oldColDefMap.get(colName);
							if (oldColDef != null) {
								if (! areColDefsSame(entry.getValue(), oldColDef)) {
									String alterDDL = "ALTER TABLE " + srcObject.getName() + " ALTER COLUMN " + DBObject.quoteColumnName(colName) + " " + newColDefMap.get(colName);
									newDDLs.add(alterDDL);
								}
							} else {
								String addColDDL = "ALTER TABLE " + srcObject.getName() + " ADD COLUMN " + DBObject.quoteColumnName(colName) + " " + entry.getValue();
								newDDLs.add(addColDDL);
							}
						}

						for (Map.Entry<String, String> entry : oldColDefMap.entrySet()) {
							String colName = entry.getKey();
							String newColDef = newColDefMap.get(colName);
							if (newColDef == null) {
								String dropDDL = "ALTER TABLE " + srcObject.getName() + " DROP COLUMN " + DBObject.quoteColumnName(colName);
								newDDLs.add(dropDDL);
							}
						}					

						if (!newDDLs.isEmpty()) {
							//Publish DDLs in the device 
							try(Connection deviceConn = DriverManager.getConnection(syncLiteDeviceURL)) {
								try(TelemetryStatement deviceStmt = (TelemetryStatement) deviceConn.createStatement()) {
									for (String ddl : newDDLs) {
										this.tracer.info("Publishing inferred DDL on device : " + ddl );
										deviceStmt.execute(ddl);
									}							
								}
							} catch (SQLException e) {
								//Following check to add imdempotent behavior to ADD COLUMN and DROP COLUMN sqls
								if (e.getMessage().contains("duplicate column name") || e.getMessage().contains("no such column")) {
									//Ignore for idempotency
								} else {
									this.tracer.error("Failed to execute inferred ddl for object " + srcObject.getFullName(), e);
									throw new SyncLiteException("Failed to execute inferred ddl for object : "  + srcObject.getFullName(), e);
								}
							}
							try {
								//Refresh table object with latest schema definition.
								srcObject.initializeDBObject(newObjectInfo.name, newObjectInfo.columnDefStr, newObjectInfo.uniqueKeyColumnsStr, srcObject.getIncrementalKeyColumnStr(), srcObject.getDeviceName(), srcObject.getPosition(), srcObject.getMaskColumnsStr(), srcObject.getDeleteCondition(), srcObject.getPartitionIdx(), srcObject.getSelectCoditionsJson(), srcObject.getSelectCoditions());

								//
								//WE NEED TO PUBLISH REFRESH TABLE ALSO SINCE COLUMNS MAY HAVE GOT REORDRED NOW.
								//REFRESH TABLE operation recreates the table in the local device with the given sequence 
								//and also does the same on replica device when applied by consolidator along with 
								//updating Table metadata in consolidator
								//So, the dbreader metadata, local device file, replica device file and consolidator metadata
								//file all get in sync with respect to the column order
								//
								try(Connection deviceConn = DriverManager.getConnection(syncLiteDeviceURL)) {
									try(TelemetryStatement deviceStmt = (TelemetryStatement) deviceConn.createStatement()) {
										deviceStmt.execute(srcObject.getRefreshTableSql());

										//Read last published commitid now.
										try (ResultSet commitIDRS = deviceStmt.executeQuery("SELECT commit_id FROM synclite_txn")) {
											if (commitIDRS.next()) {
												srcObject.setLastPublishedCommitID(commitIDRS.getLong("commit_id"));
											}
										} catch (SQLException e) {
											//Ignore
										}
									}
								} catch (SQLException e) {
									this.tracer.error("Failed to execute refresh table sql for object : " + srcObject.getFullName(), e);
									throw new SyncLiteException("Failed to execute refresh table sql for object : " + srcObject.getFullName(), e);
								}

								//Update latest schema in metadata file
								updateSrcObjectInfoInMetadataTable(srcObject);

								srcObject.incrInferredDDLCount(newDDLs.size());
							} catch (Exception e) {
								throw new SyncLiteException("Failed to update object info in metadata file for table : " + srcObject.getFullName(), e);
							}
						}
					}
				}
			}

			//Incremental replication
			incrementalRead(reloadingObject);
		}
	}

	protected void logRead() throws SyncLiteException {
		try {
			String syncLiteDeviceURL = "jdbc:synclite_telemetry:" + srcObject.getDeviceFilePath();

			//Incremental replication
			tracer.debug("Starting log read on object : " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx());			

			//update stats
			srcObject.setLastReadStartTime(System.currentTimeMillis());
			srcObject.setLastReadStatus("");
			srcObject.setLastReadStatusDescription("");
			srcObject.setLastReadEndTime(0);
			srcObject.setLastReadRecordsFetched(0);
			Monitor.getInstance().registerChangedObject(srcObject);

			long totalRecCount = logReadInternal();

			if (totalRecCount == 0) {
				//Update stats
				srcObject.setLastReadEndTime(System.currentTimeMillis());
				srcObject.setLastReadStatus("SUCCESS");
				srcObject.setLastReadStatusDescription("No data found to replicate");
				srcObject.setLastReadRecordsFetched(0);
				Monitor.getInstance().registerChangedObject(srcObject);	
			} else {
				srcObject.setLastReadEndTime(System.currentTimeMillis());
				srcObject.setLastReadStatus("SUCCESS");
				srcObject.setLastReadStatusDescription("");
				Monitor.getInstance().registerChangedObject(srcObject);	
			}
			//deviceConn.commit();
			tracer.debug("Finished log read on object : " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + ", read record count : " + totalRecCount);
		} catch (Exception e) {
			String errorMsg = e.getMessage();
			//Update stats
			srcObject.setLastReadEndTime(System.currentTimeMillis());
			srcObject.setLastReadStatus("ERROR");
			srcObject.setLastReadStatusDescription(errorMsg);
			srcObject.setLastReadRecordsFetched(0);
			Monitor.getInstance().registerChangedObject(srcObject);	
			this.tracer.error("Failed to read log for source object " +  srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() +  " and write to SyncLite device : " + e.getMessage(), e);
			throw new SyncLiteException("Failed to read log for source object " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + " and write to SyncLite device : " + e.getMessage(), e);
		}
	}

	protected long logReadInternal() throws SyncLiteException {
		throw new SyncLiteException("LOG_BASED Source DBreader method not available for current source");
	}

	protected void incrementalRead(boolean reloadingObject) throws SyncLiteException {
		try {
			String syncLiteDeviceURL = "jdbc:synclite_telemetry:" + srcObject.getDeviceFilePath();

			//Incremental replication
			tracer.info("Starting incremental read on object : " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx());			

			//update stats
			srcObject.setLastReadStartTime(System.currentTimeMillis());
			srcObject.setLastReadStatus("");
			srcObject.setLastReadStatusDescription("");
			srcObject.setLastReadEndTime(0);
			srcObject.setLastReadRecordsFetched(0);
			Monitor.getInstance().registerChangedObject(srcObject);

			long totalRecCount = incrementalReadInternal();

			if (totalRecCount == 0) {
				//Update stats
				srcObject.setLastReadEndTime(System.currentTimeMillis());
				srcObject.setLastReadStatus("SUCCESS");
				srcObject.setLastReadStatusDescription("No data found to replicate");
				srcObject.setLastReadRecordsFetched(0);
				Monitor.getInstance().registerChangedObject(srcObject);	
			} else {
				srcObject.setLastReadEndTime(System.currentTimeMillis());
				srcObject.setLastReadStatus("SUCCESS");
				srcObject.setLastReadStatusDescription("");
				if (reloadingObject) {
					srcObject.incrFullReloadCount(1);
				}
				Monitor.getInstance().registerChangedObject(srcObject);	
			}
			//deviceConn.commit();
			tracer.info("Finished incremental read on object : " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + ", read record count : " + totalRecCount);
		} catch (Exception e) {
			String errorMsg = e.getMessage();
			//Update stats
			srcObject.setLastReadEndTime(System.currentTimeMillis());
			srcObject.setLastReadStatus("ERROR");
			srcObject.setLastReadStatusDescription(errorMsg);
			srcObject.setLastReadRecordsFetched(0);
			Monitor.getInstance().registerChangedObject(srcObject);	
			this.tracer.error("Failed to read data from source object " +  srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() +  " and write to SyncLite device : " + e.getMessage(), e);
			throw new SyncLiteException("Failed to read data from source object " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + " and write to SyncLite device : " + e.getMessage(), e);
		}
	}

	protected long incrementalReadInternal() throws SyncLiteException {
		try {
			//Execute a max query to get max value of incremental key from source db object
			HashMap<String,String> maxIncrementalKeyColumnVals = new HashMap<String, String>();
			boolean computeMaxIncrementalKeyValsInDB = ConfLoader.getInstance().getSrcComputeMaxIncrementalKeyInDB();
			String syncLiteDeviceURL = "jdbc:synclite_telemetry:" + srcObject.getDeviceFilePath();

			String selectSql = srcObject.getSelectTableSql();
			if (computeMaxIncrementalKeyValsInDB) {
				tracer.info("Starting max query : " + srcObject.getMaxSql());			
				try(Connection srcConn = JDBCConnector.getInstance().connect()) {
					try(Statement srcStmt = srcConn.createStatement()) {
						try(ResultSet srcRS = srcStmt.executeQuery(srcObject.getMaxSql())) {
							if (srcRS.next()) {
								int idx=1;
								boolean allNulls = true;
								for (String incrKey : srcObject.getIncrementalKeyColumns()) {
									String maxVal = srcRS.getString(idx);
									if (maxVal != null) {
										allNulls = false;
										maxIncrementalKeyColumnVals.put(incrKey, maxVal);
									} else {
										//
										//If null then add initial value as MAX 
										//to make the condition always a negation for this table
										//
										maxIncrementalKeyColumnVals.put(incrKey, srcObject.getInitialIncrKeyColVal(incrKey));
									}
									++idx;
								}

								if (allNulls) {
									this.tracer.info("No data to replicate in object : " + srcObject.getFullName());

									return 0;
								}
							} else {
								this.tracer.info("No data to replicate in object : " + srcObject.getFullName());

								return 0;
							}
						}
					}				
				} catch(Exception e) {
					this.tracer.error("Failed to execute max query " + srcObject.getMaxSql() + " on source object " + srcObject.getFullName(), e);
					throw new SyncLiteException("Failed to execute max query " + srcObject.getMaxSql() + " on source object " + srcObject.getFullName(), e);
				}
				tracer.info("Finished max query : " + srcObject.getMaxSql() + ", read max value for incremental key columns : ");				
				int idx = 1;
				for (String incrKey : srcObject.getIncrementalKeyColumns()) {
					String incrKeyValue = maxIncrementalKeyColumnVals.get(incrKey); 
					tracer.info("Incremental key column name : "  + incrKey + ", max value : " + incrKeyValue);
					selectSql = selectSql.replace("$" + String.valueOf(idx), srcObject.getLastReadIncrementalKeyColVal(incrKey)).replace("$" + String.valueOf(idx + 1), incrKeyValue);
					idx = idx + 2;
				}
			} else {
				int idx = 1;
				for (String incrKey : srcObject.getIncrementalKeyColumns()) {
					String incrKeyValue = maxIncrementalKeyColumnVals.get(incrKey); 
					selectSql = selectSql.replace("$" + String.valueOf(idx), srcObject.getLastReadIncrementalKeyColVal(incrKey));
					++idx;
				}				
			}

			long batchRecCount = 0;
			long totalRecCount = 0;
			long batchCount = 1;
			long batchSize = ConfLoader.getInstance().getSrcDBReaderBatchSize();
			HashMap<String, IncrementalKeyType> incKeyTypes = srcObject.getIncrementalKeyTypes();
			HashMap<String,Double> computedMaxIncrementalKeyColumnVals = null;
			if (! computeMaxIncrementalKeyValsInDB) {
				computedMaxIncrementalKeyColumnVals = new HashMap<String, Double>();
				for (String incrKey : srcObject.getIncrementalKeyColumns()) {
					computedMaxIncrementalKeyColumnVals.put(incrKey, Double.MIN_VALUE);
				}
			}			
			
			tracer.info("Source object query : " + selectSql);
			try(Connection srcConn = JDBCConnector.getInstance().connect();
					Connection deviceConn = DriverManager.getConnection(syncLiteDeviceURL)) 
			{
				//
				//Disabling transaction for now as we are fine with repeated read of data as we anyways do idempotent data ingestion on destination.
				//Disabling transactions help us flush log files faster without making them wait for complete transaction.				
				//
				//deviceConn.setAutoCommit(false);
				try(Statement srcStmt = srcConn.createStatement();
						PreparedStatement devicePreparedStmt = deviceConn.prepareStatement(srcObject.getInsertTableSql());
						TelemetryStatement deviceStmt = (TelemetryStatement) deviceConn.createStatement()) 
				{
					try(ResultSet srcRS = srcStmt.executeQuery(selectSql)) {
						ResultSetMetaData metaData = srcRS.getMetaData();
						int columnCount = metaData.getColumnCount();

						while (srcRS.next()) {
							for (int i = 1; i <= columnCount; ++i) {
								if (metaData.getColumnType(i) == Types.BLOB) {
									// Treat as a blob (byte array)
									Blob blob = srcRS.getBlob(i);
									if (blob != null) {
										byte[] dstVal = blob.getBytes(1, (int) blob.length());
										devicePreparedStmt.setBytes(i, dstVal);
									} else {
										// Handle null BLOB values
										devicePreparedStmt.setNull(i, Types.BLOB);
									}
								} else if ((metaData.getColumnType(i) == Types.BOOLEAN) || (metaData.getColumnType(i) == Types.BIT)) {
									//Boolean's getString returns different values for different databases.
									//Hence using the specific method for boolean.
									if (srcRS.getString(i) == null) {
										devicePreparedStmt.setNull(i, Types.VARCHAR);
									} else {
										Boolean val = srcRS.getBoolean(i);
										String numericVal = (val == true) ? "1":"0";
										devicePreparedStmt.setString(i, numericVal);
									}
								} else {

									// Treat everything else as a string
									String val = valMasker.maskValue(srcObject, metaData.getColumnName(i), srcRS.getString(i));
									if (val != null) {
										devicePreparedStmt.setString(i, val);
									} else {
										devicePreparedStmt.setNull(i, Types.VARCHAR);					                	
									}
									
									//
									if (! computeMaxIncrementalKeyValsInDB) {
										//Check if this column is an incremental key column. 
										//If yes then convert it to numeric value (if its type is ts)
										//Maintain current max. 
										String colName = metaData.getColumnName(i);
										IncrementalKeyType keyType  = incKeyTypes.get(colName.toUpperCase());
										if (keyType != null) {
											//colName is defined as incremental key.
											Double numericColVal = Double.MIN_VALUE;
											if (keyType == IncrementalKeyType.TIMESTAMP) {
												//Try to get epoch in millis for this timestamp field
												numericColVal = convertTimestampToMills(val);
											} else {
												try {
													numericColVal = Double.valueOf(val);
												} catch (Exception e) {
													numericColVal = Double.MIN_VALUE;
												}
											}
											
											//Now compare with current max and update max as needed.
											Double currentMax = computedMaxIncrementalKeyColumnVals.get(colName);
											if (currentMax != null) {
												if (currentMax < numericColVal) {
													computedMaxIncrementalKeyColumnVals.put(colName, numericColVal);
													maxIncrementalKeyColumnVals.put(colName, val);
												}
											}
										}
									}
								}
							}
							devicePreparedStmt.addBatch();
							++batchRecCount;
							++totalRecCount;
							if (batchRecCount == batchSize) {
								++batchCount;
								tracer.info("Flushing batch number " + batchCount + " for object : " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + ", batch size : " + batchRecCount);
								devicePreparedStmt.executeBatch();

								srcObject.incrLastReadRecordsFetched(batchRecCount);
								Monitor.getInstance().registerChangedObject(srcObject);	

								batchRecCount = 0;
							}
						}
					}

					if (batchRecCount > 0) {
						tracer.info("Flushing last batch number " + batchCount + " for object : " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + ", batch size : " + batchRecCount);
						devicePreparedStmt.executeBatch();

						srcObject.incrLastReadRecordsFetched(batchRecCount);
						Monitor.getInstance().registerChangedObject(srcObject);	

					}

					if (totalRecCount > 0) {
						//
						//Update all entries.
						//
						for (Map.Entry<String, String> entry : maxIncrementalKeyColumnVals.entrySet()) {						
							if (entry.getValue() != null) {
								deviceStmt.executeUnlogged("UPDATE synclite_dbreader_checkpoint SET column_value = '" + entry.getValue() + "' WHERE object_name = '" + srcObject.getName() + "' AND column_name = '" + entry.getKey() + "'");
								srcObject.setLastReadIncrementalKeyColVal(entry.getKey(), entry.getValue());
							}
						}

						//If we found any modified records and this object has a soft delete condition then log a DELETE statement too
						if ((srcObject.getSoftDeleteBasedDeleteTableSql() != null)) {
							deviceStmt.execute(srcObject.getSoftDeleteBasedDeleteTableSql());
						}

						//Publish a FINISHBATCH record
						deviceStmt.log("FINISHBATCH " + srcObject.getName());

						//Read last published commitid now.
						try (ResultSet commitIDRS = deviceStmt.executeQuery("SELECT commit_id FROM synclite_txn")) {
							if (commitIDRS.next()) {
								srcObject.setLastPublishedCommitID(commitIDRS.getLong("commit_id"));
							}
						} catch (SQLException e) {
							//Ignore
						}
					}
					
				}
			}

			return totalRecCount;
		} catch (Exception e) {
			this.tracer.error("Failed to read data from source for object : " + srcObject.getName() + " : " + e.getMessage(), e);
			throw new SyncLiteException("Failed to read data from source for object : " + srcObject.getName() + " : " + e.getMessage(), e);
		}
	}

	protected Double convertTimestampToMills(String val) {
		try {
			//Try with one format for now.
			//We will make this more robust by handling other possible formats.
			//
			Timestamp ts = Timestamp.valueOf(val);
			Double ret = (double) ts.toInstant().toEpochMilli();
			return ret;
		} catch (Exception e) {
			//Try parsing as a numeric value 
			
			try {
				Double ret = Double.parseDouble(val);
				return ret;
			} catch (Exception e1) {
				//Ignore
			}
			return Double.MIN_VALUE;
		}
	}

	private void fullRead(boolean reloadingObject) throws SyncLiteException {
		try {
			srcObject.setLastReadStartTime(System.currentTimeMillis());
			srcObject.setLastReadEndTime(0);
			srcObject.setLastReadStatus("");
			srcObject.setLastReadStatusDescription("");
			srcObject.setLastReadRecordsFetched(0);
			Monitor.getInstance().registerChangedObject(srcObject);	

			long totalRecCount = fullReadInternal();

			//Update stats
			srcObject.setLastReadEndTime(System.currentTimeMillis());
			srcObject.setLastReadStatus("SUCCESS");
			if (totalRecCount == 0) {
				srcObject.setLastReadStatusDescription("No data found to replicate");
			} else {
				srcObject.setLastReadStatusDescription("");
			}					
			if (reloadingObject) {
				srcObject.incrFullReloadCount(1);
			}
			Monitor.getInstance().registerChangedObject(srcObject);	
		
			tracer.info("Finished full read on object : " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + ", read record count : " + totalRecCount);
		} catch (Exception e) {
			//Update stats
			String errorMsg = e.getMessage();
			srcObject.setLastReadEndTime(System.currentTimeMillis());
			srcObject.setLastReadStatus("ERROR");
			srcObject.setLastReadStatusDescription(errorMsg);
			srcObject.setLastReadRecordsFetched(0);
			Monitor.getInstance().registerChangedObject(srcObject);	

			this.tracer.error("Failed to read data from source object and write to SyncLite device : " + errorMsg, e);
			throw new SyncLiteException("Failed to read data from source object " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + " and write to SyncLite device : " + errorMsg, e);
		}
	}

	protected long fullReadInternal() throws SyncLiteException {
		try {
			String syncLiteDeviceURL = "jdbc:synclite_telemetry:" + srcObject.getDeviceFilePath();
			long batchRecCount = 0;
			long totalRecCount = 0;
			long batchCount = 1;

			long batchSize = ConfLoader.getInstance().getSrcDBReaderBatchSize();
			tracer.info("Source object query : " + srcObject.getSelectTableSql());
			try(Connection srcConn = JDBCConnector.getInstance().connect();
					Connection deviceConn = DriverManager.getConnection(syncLiteDeviceURL)) 
			{
				//deviceConn.setAutoCommit(false);
				try(Statement srcStmt = srcConn.createStatement();
						PreparedStatement devicePreparedStmt = deviceConn.prepareStatement(srcObject.getInsertTableSql());
						TelemetryStatement deviceStmt = (TelemetryStatement) deviceConn.createStatement()) 
				{
					try(ResultSet srcRS = srcStmt.executeQuery(srcObject.getSelectTableSql())) {
						ResultSetMetaData metaData = srcRS.getMetaData();
						int columnCount = metaData.getColumnCount();

						while (srcRS.next()) {
							for (int i = 1; i <= columnCount; ++i) {
								int t = metaData.getColumnType(i);
								if (metaData.getColumnType(i) == Types.BLOB) {
									// Treat as a blob (byte array)
									Blob blob = srcRS.getBlob(i);
									if (blob != null) {
										byte[] dstVal = blob.getBytes(1, (int) blob.length());
										devicePreparedStmt.setBytes(i, dstVal);
									} else {
										// Handle null BLOB values
										devicePreparedStmt.setNull(i, Types.BLOB);
									}
								} else if ((metaData.getColumnType(i) == Types.BOOLEAN) || (metaData.getColumnType(i) == Types.BIT)) {
									//Boolean's getString returns different values for different databases.
									//Hence using the specific method for boolean.
									//Boolean's getString returns different values for different databases.
									//Hence using the specific method for boolean.
									if (srcRS.getString(i) == null) {
										devicePreparedStmt.setNull(i, Types.VARCHAR);
									} else {
										Boolean val = srcRS.getBoolean(i);
										String numericVal = (val == true) ? "1":"0";
										devicePreparedStmt.setString(i, numericVal);
									}
								} else {
									// Treat everything else as a string
									String val = valMasker.maskValue(srcObject,metaData.getColumnName(i), srcRS.getString(i));
									if (val != null) {
										devicePreparedStmt.setString(i, val);
									} else {
										devicePreparedStmt.setNull(i, Types.VARCHAR);					                	
									}
								}
							}
							devicePreparedStmt.addBatch();
							++batchRecCount;
							++totalRecCount;
							if (batchRecCount == batchSize) {
								tracer.info("Flushing batch number " + batchCount + " for object : " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + ", batch size : " + batchRecCount);
								devicePreparedStmt.executeBatch();

								//update stats
								srcObject.incrLastReadRecordsFetched(batchRecCount);
								Monitor.getInstance().registerChangedObject(srcObject);	

								batchRecCount = 0;
								++batchCount;
							}
						}
						if (batchRecCount > 0) {
							tracer.info("Flushing last batch number : " + batchCount  + " for object : " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + ", batch size : " + batchRecCount);
							devicePreparedStmt.executeBatch();

							//update stats
							srcObject.incrLastReadRecordsFetched(batchRecCount);
							Monitor.getInstance().registerChangedObject(srcObject);	
						}
					}

					if (totalRecCount > 0) {						

						//Just update the single entry with Long.MAX_VALUE.
						deviceStmt.executeUnlogged("UPDATE synclite_dbreader_checkpoint SET column_value = '" + Long.MAX_VALUE + "' WHERE object_name = '" + srcObject.getName() + "'");
						srcObject.setLastReadIncrementalKeyColVal("", String.valueOf(Long.MAX_VALUE));

						//Publish a FINISHBATCH record
						deviceStmt.log("FINISHBATCH " + srcObject.getName());

						//Read last published commitid now.
						try (ResultSet commitIDRS = deviceStmt.executeQuery("SELECT commit_id FROM synclite_txn")) {
							if (commitIDRS.next()) {
								srcObject.setLastPublishedCommitID(commitIDRS.getLong("commit_id"));
							}
						} catch (SQLException e) {
							//Ignore
						}
					}
					
				}		
			}
			return totalRecCount;
		} catch (Exception e) {
			this.tracer.error("Failed to read data from source for object : " + srcObject.getName() + " : " + e.getMessage(), e);
			throw new SyncLiteException("Failed to read data from source for object : " + srcObject.getName() + " : " + e.getMessage(), e);
		}
	}

	protected void fullReadKeys() throws SyncLiteException {		
		try {
			tracer.info("Starting full key reading on object : " + srcObject.getFullName());
			//Full object key read
			srcObject.setLastDeleteSyncStartTime(System.currentTimeMillis());
			srcObject.setLastDeleteSyncKeysFetched(0);
			Monitor.getInstance().registerChangedObject(srcObject);	

			long totalRecCount = fullReadKeysInternal();

			//update stats
			srcObject.setLastDeleteSyncEndTime(System.currentTimeMillis());
			srcObject.setLastReadStatus("SUCCESS");
			if (totalRecCount == 0) {
				srcObject.setLastReadStatusDescription("No data found to replicate");
			} else {
				srcObject.setLastReadStatusDescription("");
			}					

			Monitor.getInstance().registerChangedObject(srcObject);	

			tracer.info("Finished key reading on object : " + srcObject.getFullName() + ", read record count : " + totalRecCount);
		} catch (Exception e) {
			String errorMsg = e.getMessage();

			//Update stats
			srcObject.setLastDeleteSyncEndTime(System.currentTimeMillis());
			srcObject.setLastReadStatus("ERROR");
			srcObject.setLastReadStatusDescription(errorMsg);
			srcObject.setLastDeleteSyncKeysFetched(0);
			Monitor.getInstance().registerChangedObject(srcObject);	

			this.tracer.error("Failed to read keys from source object and write to SyncLite device : " + e.getMessage(), e);
			throw new SyncLiteException("Failed to read keys from source object " + srcObject.getFullName() + "and write to SyncLite device : " + e.getMessage(), e);
		}
	}

	protected long fullReadKeysInternal() throws SyncLiteException {
		String syncLiteDeviceURL = "jdbc:synclite_telemetry:" + srcObject.getDeviceFilePath();
		tracer.info("Source object query : " + srcObject.getSelectKeyTableSql());
		long batchRecCount = 0;
		long totalRecCount = 0;
		long batchCount = 1;
		long batchSize = ConfLoader.getInstance().getSrcDBReaderBatchSize();
		try(Connection srcConn = JDBCConnector.getInstance().connect();
				Connection deviceConn = DriverManager.getConnection(syncLiteDeviceURL)) 
		{
			//deviceConn.setAutoCommit(false);

			try(Statement srcStmt = srcConn.createStatement();
					TelemetryStatement deviceStmt = (TelemetryStatement) deviceConn.createStatement()) 
			{
				deviceStmt.execute(srcObject.getDropKeyTableSql());
				deviceStmt.execute(srcObject.getCreateKeyTableSql());
				try (PreparedStatement devicePreparedStmt = deviceConn.prepareStatement(srcObject.getInsertKeyTableSql())) {
					try(ResultSet srcRS = srcStmt.executeQuery(srcObject.getSelectKeyTableSql())) {
						ResultSetMetaData metaData = srcRS.getMetaData();
						int columnCount = metaData.getColumnCount();

						while (srcRS.next()) {
							for (int i = 1; i <= columnCount; ++i) {
								if (metaData.getColumnType(i) == Types.BLOB) {
									// Treat as a blob (byte array)
									Blob blob = srcRS.getBlob(i);
									if (blob != null) {
										byte[] dstVal = blob.getBytes(1, (int) blob.length());
										devicePreparedStmt.setBytes(i, dstVal);
									} else {
										// Handle null BLOB values
										devicePreparedStmt.setNull(i, Types.BLOB);
									}
								} else if ((metaData.getColumnType(i) == Types.BOOLEAN) || (metaData.getColumnType(i) == Types.BIT)) {
									//Boolean's getString returns different values for different databases.
									//Hence using the specific method for boolean.
									//Boolean's getString returns different values for different databases.
									//Hence using the specific method for boolean.
									if (srcRS.getString(i) == null) {
										devicePreparedStmt.setNull(i, Types.VARCHAR);
									} else {
										Boolean val = srcRS.getBoolean(i);
										String numericVal = (val == true) ? "1":"0";
										devicePreparedStmt.setString(i, numericVal);
									}
								} else {
									// Treat everything else as a string
									String val = srcRS.getString(i);
									if (val!= null) {
										devicePreparedStmt.setString(i, val);
									} else {
										devicePreparedStmt.setNull(i, Types.VARCHAR);					                	
									}
								}
							}
							devicePreparedStmt.addBatch();
							++batchRecCount;
							++totalRecCount;
							if (batchRecCount == batchSize) {
								tracer.info("Flushing batch number " + batchCount + " for object : " + srcObject.getFullName() + ", batch size : " + batchRecCount);
								devicePreparedStmt.executeBatch();

								srcObject.incrLastDeleteSyncKeysFetched(batchRecCount);
								Monitor.getInstance().registerChangedObject(srcObject);	

								batchRecCount = 0;
								++batchCount;
							}
						}
						if (batchRecCount > 0) {
							tracer.info("Flushing last batch number : " + batchCount  + " for object : " + srcObject.getFullName() + ", batch size : " + batchRecCount);
							devicePreparedStmt.executeBatch();

							srcObject.incrLastDeleteSyncKeysFetched(batchRecCount);
							Monitor.getInstance().registerChangedObject(srcObject);
						}
					}
				}

				if (totalRecCount > 0) {
					deviceStmt.log(srcObject.getKeyBasedDeleteTableSql());

					deviceStmt.execute(srcObject.getDropKeyTableSql());
					deviceStmt.log("FINISHBATCH " + srcObject.getName());

					//Read last published commitid now.
					try (ResultSet commitIDRS = deviceStmt.executeQuery("SELECT commit_id FROM synclite_txn")) {
						if (commitIDRS.next()) {
							srcObject.setLastPublishedCommitID(commitIDRS.getLong("commit_id"));
						}
					} catch (SQLException e) {
						//Ignore
					}
				}
				
			}	
		} catch (Exception e) {
			throw new SyncLiteException("Failed to read data from source for object : " + srcObject.getName() + " : " + e.getMessage(), e);
		}
		return totalRecCount;
	}


	public void processDeleteSync() throws SyncLiteException {
		String syncLiteDeviceURL = "jdbc:synclite_telemetry:" + srcObject.getDeviceFilePath();
		if (srcObject.hasUniqueKeyDefined()) {
			fullReadKeys();
		}
	}

	private final boolean areColDefsSame(String newColDef, String oldColDef) {
		newColDef = newColDef.replaceAll("\\s+", "").toUpperCase();
		oldColDef = oldColDef.replaceAll("\\s+", "").toUpperCase();
		if (newColDef.equals(oldColDef)) {
			return true;
		}
		return false;
	}


	private void updateSrcObjectInfoInMetadataTable(DBObject object) throws SyncLiteException {
		Path metadataFilePath = ConfLoader.getInstance().getSyncLiteDeviceDir().resolve("synclite_dbreader_metadata.db");
		String url = "jdbc:sqlite:" + metadataFilePath;
		String metadataTableUpdateSql = "UPDATE src_object_info SET allowed_columns = ?, unique_key_columns = ? WHERE object_name = ?";

		while (true) {
			try (Connection conn = DriverManager.getConnection(url)){
				conn.setAutoCommit(false);
				try (PreparedStatement updatePstmt = conn.prepareStatement(metadataTableUpdateSql)) {
					updatePstmt.setString(1, srcObject.getColumnDefJson());
					updatePstmt.setString(2, srcObject.getUniqueKeyColumnsStr());				
					updatePstmt.setString(3, srcObject.getName());
					updatePstmt.addBatch();					
					updatePstmt.executeBatch();
				}
				conn.commit();
				this.tracer.info("Updated object info for object : " + srcObject.getFullName() + " in metadata file");
				break;
			} catch (SQLException e) {
				if (e.getSQLState().equals("SQLITE_BUSY")) {
					this.tracer.error("Failed to update object info for object : " + srcObject.getFullName() + " in metadata file as it is busy : " + metadataFilePath + ", retrying..");
					continue;
				}
				this.tracer.error("Failed to update object info for object : " + srcObject.getFullName() + " in metadata file : " + metadataFilePath, e);
				throw new SyncLiteException("Failed to update object info for object : " + srcObject.getFullName() + " in metadata file : " + metadataFilePath, e);
			}
		}

	}

	private final void deleteSrcTableInfoInMetadataFile(DBObject object) throws SyncLiteException {
		Path metadataFilePath = ConfLoader.getInstance().getSyncLiteDeviceDir().resolve("synclite_dbreader_metadata.db");
		String url = "jdbc:sqlite:" + metadataFilePath;
		String metadataTableDeleteSql = "DELETE FROM src_object_info WHERE object_name = ?";
		while (true) {
			try (Connection conn = DriverManager.getConnection(url)){
				try(PreparedStatement deletePstmt = conn.prepareStatement(metadataTableDeleteSql)) {
					deletePstmt.setString(1, srcObject.getName());
					deletePstmt.addBatch();
					deletePstmt.executeBatch();
				}
				this.tracer.info("Removed object info for object : " + object + " from metadata file");
				break;
			} catch (SQLException e) {
				if (e.getSQLState().equals("SQLITE_BUSY")) {
					this.tracer.error("Failed to delete object info for object : " + srcObject.getFullName() + " in metadata file as it is busy : " + metadataFilePath + ", retrying..");
					continue;
				}
				this.tracer.error("Failed to delete object info for object : " + srcObject.getFullName() + " in metadata file : " + metadataFilePath, e);
				throw new SyncLiteException("Failed to delete object info for object : " + srcObject.getFullName() + " in metadata file : " + metadataFilePath, e);
			}
		}
	}

	

	public static DBReader getInstance(DBObject object, Logger tracer) throws SyncLiteException {
		switch (ConfLoader.getInstance().getSrcType()) {
		case CSV:
			return new CSVFileReader(object, tracer);
		case MONGODB:
			return new MongoDBReader(object, tracer);
		default:
			return new DBReader(object, tracer);
		}
	}
}
