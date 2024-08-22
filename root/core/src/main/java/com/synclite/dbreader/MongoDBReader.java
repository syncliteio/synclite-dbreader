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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;

import io.synclite.logger.*;

public class MongoDBReader extends DBReader {
	
	//Only one object of fileStorage to be used by all readers	
	private static MongoClient mongoClient = createMongoClient();
	private static MongoDatabase mongoDatabase = mongoClient.getDatabase(ConfLoader.getInstance().getSrcDatabase());
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
    

	public MongoDBReader(DBObject object, Logger tracer) throws SyncLiteException {
		super(object, tracer);
		//sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	private static MongoClient createMongoClient() {
		return MongoClients.create(ConfLoader.getInstance().getSrcConnectionString());
	}

	static MongoClient getMongoClient() {
		return mongoClient;
	}
	
	protected long fullReadKeysInternal() throws SyncLiteException {
		String syncLiteDeviceURL = "jdbc:synclite_telemetry:" + srcObject.getDeviceFilePath();
		tracer.info("Source object query : " + srcObject.getSelectKeyTableSql());
		long batchRecCount = 0;
		long totalRecCount = 0;
		long batchCount = 1;
		long batchSize = ConfLoader.getInstance().getSrcDBReaderBatchSize();
		try(Connection deviceConn = DriverManager.getConnection(syncLiteDeviceURL))	{
			//deviceConn.setAutoCommit(false);
			try(TelemetryStatement deviceStmt = (TelemetryStatement) deviceConn.createStatement()) {
				deviceStmt.execute(srcObject.getDropKeyTableSql());
				deviceStmt.execute(srcObject.getCreateKeyTableSql());
				try (PreparedStatement devicePreparedStmt = deviceConn.prepareStatement(srcObject.getInsertKeyTableSql())) {
					MongoCollection<Document> collection = mongoDatabase.getCollection(srcObject.getName());
					// Find all documents in the collection
					
					HashMap<Integer, String> selectConditions = srcObject.getSelectCoditions();
					FindIterable<Document> documents = null;
					if ((selectConditions != null) && (selectConditions.size() > 0)) {
						Document filter = new Document();
						List<Document> conditionDocs = new ArrayList<Document>();
						for (String condition : selectConditions.values()) {
							conditionDocs.add(Document.parse(condition));
						}
						filter.append("$or", conditionDocs);
						documents = collection.find(filter);
					} else {
						documents = collection.find();
					}
					
					for (Document document : documents) {
						String id = document.getObjectId("_id").toString();
						devicePreparedStmt.setString(1, id);
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
			}
			return totalRecCount;
		} catch (Exception e) {
			throw new SyncLiteException("Failed to read data from source for object : " + srcObject.getName() + " : " + e.getMessage(), e);
		}
	}
	
	private LogPosition getInitialLogPosition() throws SyncLiteException {
		try {
			MongoDatabase localDatabase = mongoClient.getDatabase("local");

			// Access the oplog.rs collection
			MongoCollection<Document> oplogCollection = localDatabase.getCollection("oplog.rs");

			  var pipeline = List.of(
		                new Document("$sort", new Document("ts", -1)),
		                new Document("$limit", 1),
		                new Document("$project", new Document("ts", 1).append("_id", 0))
		            );
			  
			// Execute the aggregation pipeline
			var cursor = oplogCollection.aggregate(pipeline).iterator();
			LogPosition pos = new LogPosition();
			pos.position = "";
			pos.ts = 0;
			if (cursor.hasNext()) {
				// Extract the timestamp from the first document
                var ts = cursor.next().get("ts", BsonTimestamp.class);

				pos.ts = ts.getValue();
			}
			return pos;
		} catch (Exception e) {
			throw new SyncLiteException("Failed to get latest log position for object: " + srcObject.getName() + " from MongoDB: " + e.getMessage(), e);
		}
	}
	 
/*
	private LogPosition getInitialLogPosition() throws SyncLiteException {
		LogPosition pos = new LogPosition();
		try {
			MongoDatabase database = mongoClient.getDatabase(ConfLoader.getInstance().getSrcDatabase());

			// Access the collection
			MongoCollection<Document> collection = database.getCollection(srcObject.getName());

			long currentTS = getCurrentServerTime();
			//long currentTS = 0L;
			BsonTimestamp bsonTS = new BsonTimestamp(currentTS);
			// Create a change stream on the collection
			int batchSize = Integer.valueOf(ConfLoader.getInstance().getSrcDBReaderBatchSize().toString());
			ChangeStreamIterable<Document> changeStream = collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP).startAtOperationTime(bsonTS).batchSize(batchSize);

			BsonDocument latestLogPosition = null;
			// Get the initial resume token
			try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStream.cursor()) {
				if (cursor.available() > 0) {
					if (cursor.hasNext()) {
						ChangeStreamDocument<Document> event = cursor.next();
						latestLogPosition = event.getResumeToken();
						bsonTS = event.getClusterTime();
					}
				}
			}
			pos.ts = bsonTS.getValue();
			pos.position = "";
			if (latestLogPosition != null) {
				pos.position = latestLogPosition.toString();
			}
		} catch (Exception e) {
			throw new SyncLiteException("Failed to get latest log position for object : " + srcObject.getName() + " from mongodb : " + e.getMessage(), e);
		}
		return pos;
	}
*/

	private final long getCurrentServerTime() throws SyncLiteException {
		try {
			// Assuming you have a MongoDB database instance named "database"
			MongoDatabase database = mongoClient.getDatabase("admin");

			// Execute the serverStatus command
			Document serverStatus = database.runCommand(new Document("serverStatus", 1));

			// Retrieve the current time from the serverStatus response
			Date currentTime = serverStatus.getDate("localTime");
			return currentTime.toInstant().toEpochMilli();
		} catch (Exception e) {
			throw new SyncLiteException("Failed to get current server time from MongoDB : " + e.getMessage(), e);
		}
	}

	/*
	private String getLogPosition() throws SyncLiteException {
	    try {
	        MongoDatabase localDatabase = mongoClient.getDatabase("local");

	        // Access the oplog.rs collection
	        MongoCollection<Document> oplogCollection = localDatabase.getCollection("oplog.rs");

	        // Query the oplog.rs collection to get the latest entry
	        Document latestOplogEntry = oplogCollection.find()
	                .sort(new Document("$natural", -1))
	                .limit(1)
	                .first();

	        // Get the resume token from the latest oplog entry
	        if (latestOplogEntry != null) {
	            Object resumeToken = latestOplogEntry.get("resumeToken");
	            if (resumeToken != null) {
	                return resumeToken.toString();
	            }
	        }
	        throw new SyncLiteException("No latest oplog entry found");
	    } catch (Exception e) {
	        throw new SyncLiteException("Failed to get latest log position for object: " + srcObject.getName() + " from MongoDB: " + e.getMessage(), e);
	    }
	    //return "";
	}
	 */	
	@Override
	protected long fullReadInternal() throws SyncLiteException {
		try {
			String syncLiteDeviceURL = "jdbc:synclite_telemetry:" + srcObject.getDeviceFilePath();

			long batchRecCount = 0;
			long totalRecCount = 0;
			long batchCount = 1;
			long batchSize = ConfLoader.getInstance().getSrcDBReaderBatchSize();
			//Read dataFiles one by one. and publish records.
			try(Connection deviceConn = DriverManager.getConnection(syncLiteDeviceURL)) {
				try (PreparedStatement devicePreparedStmt = deviceConn.prepareStatement(srcObject.getInsertTableSql());
						TelemetryStatement deviceStmt = (TelemetryStatement) deviceConn.createStatement()) {

					LogPosition initialLogPosition = null;
					if (ConfLoader.getInstance().getSrcDBReaderMethod() == DBReaderMethod.LOG_BASED) {
						initialLogPosition = getInitialLogPosition();
						//Update initial log position into checkpoint table					
						deviceStmt.executeUnlogged("UPDATE synclite_logreader_checkpoint SET log_position = '" + initialLogPosition.position + "', log_ts = " + initialLogPosition.ts  + " WHERE object_name = '" + srcObject.getName() + "'");
						srcObject.setLastReadLogPosition(initialLogPosition.position);
						srcObject.setLastReadLogTS(initialLogPosition.ts);
					}
					MongoCollection<Document> collection = mongoDatabase.getCollection(srcObject.getName());

					String condition = srcObject.getSelectCodition();
					FindIterable<Document> documents = null;
					if (condition != null) {
						Document filter = Document.parse(condition);
						documents = collection.find(filter);
					} else {
						// Find all documents in the collection
						documents = collection.find();
					}

					for (Document document : documents) {
						String id = document.getObjectId("_id").toString();
						String doc = valMasker.maskDocument(srcObject, document).toJson();
						devicePreparedStmt.setString(1, id);
						devicePreparedStmt.setString(2, doc);
						devicePreparedStmt.addBatch();
						++batchRecCount;
						++totalRecCount;
						if (batchRecCount == batchSize) {
							tracer.info("Flushing batch number " + batchCount + " for object : " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + ", batch size : " + batchRecCount);
							devicePreparedStmt.executeBatch();

							srcObject.incrLastReadRecordsFetched(batchRecCount);
							Monitor.getInstance().registerChangedObject(srcObject);	

							batchRecCount = 0;
							++batchCount;
						}
					}
					if (batchRecCount > 0) {
						tracer.info("Flushing last batch number : " + batchCount  + " for object : " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + ", batch size : " + batchRecCount);
						devicePreparedStmt.executeBatch();

						srcObject.incrLastReadRecordsFetched(batchRecCount);
						Monitor.getInstance().registerChangedObject(srcObject);
					}

					//Just update the single entry with Long.MAX_VALUE.
					deviceStmt.executeUnlogged("UPDATE synclite_dbreader_checkpoint SET column_value = '" + Long.MAX_VALUE + "' WHERE object_name = '" + srcObject.getName() + "'");
					srcObject.setLastReadIncrementalKeyColVal("", String.valueOf(Long.MAX_VALUE));
					if (totalRecCount > 0) {

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
			this.tracer.error("Failed to read data from MongoDB collection : " + srcObject.getName() + " : " + e.getMessage(), e);
			throw new SyncLiteException("Failed to read data from MongoDB collection : " + srcObject.getName() + " : " + e.getMessage(), e);
		}
	}

	@Override
	protected long logReadInternal() throws SyncLiteException {
		try {
			String syncLiteDeviceURL = "jdbc:synclite_telemetry:" + srcObject.getDeviceFilePath();
			HashMap<String,String> maxIncrementalKeyColumnVals = new HashMap<String, String>();

			long batchRecCount = 0;
			long totalRecCount = 0;
			long batchCount = 1;
			int batchSize = Integer.valueOf(ConfLoader.getInstance().getSrcDBReaderBatchSize().toString());
			//Read dataFiles one by one. and publish records.
			try(Connection deviceConn = DriverManager.getConnection(syncLiteDeviceURL)) {
				try (PreparedStatement deviceInsertPreparedStmt = deviceConn.prepareStatement(srcObject.getInsertTableSql());
						PreparedStatement deviceUpdatePreparedStmt = deviceConn.prepareStatement(srcObject.getUpdateTableSql());
						PreparedStatement deviceDeletePreparedStmt = deviceConn.prepareStatement(srcObject.getDeleteTableSql());
						TelemetryStatement deviceStmt = (TelemetryStatement) deviceConn.createStatement()) {

					/*
					MongoDatabase localDatabase = mongoClient.getDatabase("local");

					MongoCollection<Document> opLogCollection = mongoDatabase.getCollection("oplog.rs");

					// Find all documents in the collection
					FindIterable<Document> documents = opLogCollection.find();
					// Query oplog.rs collection for entries matching criteria
					try (var cursor = localDatabase.getCollection("oplog.rs").find(
							new Document("ns", ConfLoader.getInstance().getSrcDatabase() + "." + srcObject.getName())
							.append("ts", new Document("$gt", srcObject.getLastReadLogPosition()))
							.append("op", new Document("$in", new String[]{"i", "u", "d"})))
							.sort(new Document("$natural", 1))
							.iterator()) {

						// Iterate over oplog entries
						while (cursor.hasNext()) {
							// Get _id and document data
							Document oplogEntry = cursor.next();
							String id = oplogEntry.get("o2", Document.class).getObjectId("_id").toString();
							String doc = oplogEntry.get("o", Document.class).toString();
							devicePreparedStmt.setString(1, id);
							devicePreparedStmt.setString(2, doc);
							devicePreparedStmt.addBatch();
							++batchRecCount;
							++totalRecCount;
							++fileRecCount;
							if (batchRecCount == batchSize) {
								tracer.info("Flushing batch number " + batchCount + " for object : " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + ", batch size : " + batchRecCount);
								devicePreparedStmt.executeBatch();

								srcObject.incrLastReadRecordsFetched(batchRecCount);
								Monitor.getInstance().registerChangedObject(srcObject);	

								batchRecCount = 0;
								++batchCount;
							}
						}
					}
					 */

					MongoDatabase database = mongoClient.getDatabase(ConfLoader.getInstance().getSrcDatabase());
					MongoCollection<Document> collection = database.getCollection(srcObject.getName());

					BsonTimestamp lastReadLogTSBsonTS =  new BsonTimestamp(srcObject.getLastReadLogTS());
					ChangeStreamIterable<Document> changeStream = null;
					if (srcObject.getLastReadLogPosition().isBlank()) {
						if (srcObject.getLastReadLogTS() > 0) {
							changeStream = collection.watch()
									.fullDocument(FullDocument.UPDATE_LOOKUP)
									.startAtOperationTime(lastReadLogTSBsonTS)
									.batchSize(batchSize);
						} else {
							changeStream = collection.watch()
									.fullDocument(FullDocument.UPDATE_LOOKUP)
									.batchSize(batchSize);
						}
					} else {						
						BsonDocument resumeToken = BsonDocument.parse(srcObject.getLastReadLogPosition());
						// Create a change stream on the collection
						changeStream = collection.watch()
								.fullDocument(FullDocument.UPDATE_LOOKUP)
								.resumeAfter(resumeToken)
								.batchSize(batchSize);
					} 

					boolean hasInsertOper = false;
					boolean hasDeleteOper = false;
					boolean hasUpdateOper = false;
					BsonDocument resumeTokenDoc = null;
					BsonTimestamp lastEventBsonTS = null;
					
					try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStream.cursor()) {
						if (cursor.available() > 0) {
							while(cursor.hasNext()) {
								ChangeStreamDocument<Document> event = cursor.next();
								lastEventBsonTS = event.getClusterTime();

								/*
								if (lastEventBsonTS.compareTo(lastReadLogTSBsonTS) <= 0) {
									if (cursor.available() == 0) {
										break;
									} else {
										continue;
									}
								}
								*/
								
								OperationType operationType = event.getOperationType();
								Document document = event.getFullDocument();
								BsonDocument documentKey = event.getDocumentKey();
								String id = documentKey.getObjectId("_id").getValue().toHexString();

								String doc = "";
								if (document != null) {
									doc = valMasker.maskDocument(srcObject, document).toJson();
								}
								
								switch(operationType) {
								case INSERT:
									deviceInsertPreparedStmt.setString(1, id);
									deviceInsertPreparedStmt.setString(2, doc);
									deviceInsertPreparedStmt.addBatch();
									hasInsertOper = true;
									break;
								case UPDATE:
									deviceUpdatePreparedStmt.setString(1, id);
									deviceUpdatePreparedStmt.setString(2, "");
									deviceUpdatePreparedStmt.setString(3, id);
									deviceUpdatePreparedStmt.setString(4, doc);									
									deviceUpdatePreparedStmt.addBatch();
									hasUpdateOper = true;
									break;
								case DELETE:
									deviceDeletePreparedStmt.setString(1, id);
									deviceDeletePreparedStmt.setString(2, doc);
									deviceDeletePreparedStmt.addBatch();
									hasDeleteOper = true;
									break;
									
								}
								++batchRecCount;
								++totalRecCount;

								if (batchRecCount == batchSize) {
									tracer.info("Flushing batch number " + batchCount + " for object : " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + ", batch size : " + batchRecCount);
									if (hasInsertOper) {
										deviceInsertPreparedStmt.executeBatch();
									}
									if (hasUpdateOper) {
										deviceUpdatePreparedStmt.executeBatch();
									}
									if (hasDeleteOper) {
										deviceDeletePreparedStmt.executeBatch();
									}

									String latestLogPosition = "";
									long latestLogTS = lastEventBsonTS.getValue();
									if (resumeTokenDoc != null) {
										latestLogPosition = resumeTokenDoc.toString();
									}
									deviceStmt.executeUnlogged("UPDATE synclite_logreader_checkpoint SET log_position = '" + latestLogPosition.toString() + "', log_ts = " + latestLogTS +" WHERE object_name = '" + srcObject.getName() + "'");
									srcObject.setLastReadLogPosition(latestLogPosition.toString());
									srcObject.setLastReadLogTS(latestLogTS);

									srcObject.incrLastReadRecordsFetched(batchRecCount);
									Monitor.getInstance().registerChangedObject(srcObject);
									batchRecCount = 0;
									++batchCount;
								}
								resumeTokenDoc = cursor.getResumeToken();
							
								if (cursor.available() == 0) {
									break;
								}
							}
						}
					} catch (Exception e) {
						this.tracer.error("Failed to get a cursor from changestream for object : " + srcObject.getName() + ", operation will be retried : " + e.getMessage(), e);
						throw e;
					}

					if (batchRecCount > 0) {
						tracer.info("Flushing last batch number : " + batchCount  + " for object : " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + ", batch size : " + batchRecCount);
						if (hasInsertOper) {
							deviceInsertPreparedStmt.executeBatch();
						}
						if (hasUpdateOper) {
							deviceUpdatePreparedStmt.executeBatch();
						}
						if (hasDeleteOper) {
							deviceDeletePreparedStmt.executeBatch();
						}

						String latestLogPosition = "";
						long latestLogTS = lastEventBsonTS.getValue();
						if (resumeTokenDoc != null) {
							latestLogPosition = resumeTokenDoc.toString();
						}
						deviceStmt.executeUnlogged("UPDATE synclite_logreader_checkpoint SET log_position = '" + latestLogPosition.toString() + "', log_ts = " + latestLogTS +" WHERE object_name = '" + srcObject.getName() + "'");
						srcObject.setLastReadLogPosition(latestLogPosition.toString());
						srcObject.setLastReadLogTS(latestLogTS);

						srcObject.incrLastReadRecordsFetched(batchRecCount);
						Monitor.getInstance().registerChangedObject(srcObject);
					}

					if (totalRecCount > 0) {
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
			this.tracer.error("Failed to read log for MongoDB collection : " + srcObject.getName() + " : " + e.getMessage(), e);
			throw new SyncLiteException("Failed to read log for MongoDB collection : " + srcObject.getName() + " : " + e.getMessage(), e);
		}
	}

	@Override
	protected long incrementalReadInternal() throws SyncLiteException {
		try {
			String syncLiteDeviceURL = "jdbc:synclite_telemetry:" + srcObject.getDeviceFilePath();
			HashMap<String,String> maxIncrementalKeyColumnVals = new HashMap<String, String>();
			HashMap<String,IncrementalKeyType> incrementalKeyTypes = new HashMap<String, IncrementalKeyType>();
			incrementalKeyTypes.putAll(srcObject.getIncrementalKeyTypes());
			boolean computeMaxIncrementalKeyValsInDB = ConfLoader.getInstance().getSrcComputeMaxIncrementalKeyInDB();
			
			MongoCollection<Document> collection = mongoDatabase.getCollection(srcObject.getName());

			Document filter = null;
			String condition = srcObject.getSelectCodition();
			if (condition != null) {
				filter = Document.parse(condition);
			}

			if (ConfLoader.getInstance().getSrcComputeMaxIncrementalKeyInDB()) {
				tracer.info("Starting max query on collection : " + srcObject.getName());
				try {
					boolean allNulls = true;
					for (String incrKey : srcObject.getIncrementalKeyColumns()) {
						// Use the aggregate framework to find the maximum value of the timestamp field
						// Use the aggregate framework to find the maximum value of the timestamp field
	
						Object maxVal = null;
						IncrementalKeyType type = null;
						//Create the $match stage to filter out documents with null or missing values for the field
				        Document matchStage = new Document("$match", new Document(incrKey, new Document("$exists", true).append("$ne", null)));

				        // Create the $group stage to compute the max value of the field
				        //Document groupStage = new Document("$group", new Document("_id", null).append("last", new Document("$max", new Document("$toDate", "$" + incrKey))));
				        Document groupStage = new Document("$group", new Document("_id", null).append("last", new Document("$max", "$" + incrKey)));

				        // Perform the aggregation pipeline
				        AggregateIterable<Document> result = collection.aggregate(Arrays.asList(matchStage, groupStage));

				        // Get the result document
				        Document firstResult = result.first();				        
				        // Extract the max value from the result document
				        if (firstResult != null) {
				            Object m = firstResult.get("last");
				            if (m != null) {
				                //maxVal = sdf.format(m) + "Z";
				            	
				            	if (m instanceof Date) {
					            	maxVal = sdf.format(m);
					            	type = IncrementalKeyType.TIMESTAMP;
				            	} else {
				            		maxVal = m.toString();
					            	type = IncrementalKeyType.NUMERIC;
				            	}
				            }
				        }
				        
						if (maxVal != null) {
							allNulls = false;
							maxIncrementalKeyColumnVals.put(incrKey, maxVal.toString());
							incrementalKeyTypes.put(incrKey, type);
						} else {
							//
							//If null then add initial value as MAX 
							//to make the condition always a negation for this table
							//
							maxIncrementalKeyColumnVals.put(incrKey, srcObject.getInitialIncrKeyColVal(incrKey));
							incrementalKeyTypes.put(incrKey, type);
						}
					}
	
					if (allNulls) {
						this.tracer.info("No data to replicate in object : " + srcObject.getFullName());
	
						return 0;
					}
				}  catch(Exception e) {
					this.tracer.error("Failed to execute max query on source object " + srcObject.getFullName(), e);
				}
	
				tracer.info("Finished max query : " + srcObject.getMaxSql() + ", read max value for incremental key columns : ");
			} 

			
			HashMap<String,Double> computedMaxIncrementalKeyColumnVals = null;
			if (! computeMaxIncrementalKeyValsInDB) {
				computedMaxIncrementalKeyColumnVals = new HashMap<String, Double>();
				for (String incrKey : srcObject.getIncrementalKeyColumns()) {
					computedMaxIncrementalKeyColumnVals.put(incrKey, Double.MIN_VALUE);
					incrementalKeyTypes.put(incrKey, IncrementalKeyType.TIMESTAMP);
				}
			}

			Document predicate = new Document();
			List<Document> orPredicates = new ArrayList<>();

			boolean includeNullIncrementalKeys = ConfLoader.getInstance().getSrcReadNullIncrementalKeyRecords();
			for (String incrKey : srcObject.getIncrementalKeyColumns()) {
				IncrementalKeyType incrKeyType = incrementalKeyTypes.get(incrKey); 
				Document incrKeyNullPred = null;						
				if (includeNullIncrementalKeys) {
			        incrKeyNullPred = new Document();
			        incrKeyNullPred.append("$or", Arrays.asList(
			                new Document(incrKey, new Document("$exists", false)), // Field does not exists
			                new Document(incrKey, null) // Field value is null
			            ));
				}
				if (computeMaxIncrementalKeyValsInDB) {
					Document incrMinPred = new Document();
					String lastReadIncrKeyValue = srcObject.getLastReadIncrementalKeyColVal(incrKey);
					Object lastReadIncrObj = null;
					if (incrKeyType == IncrementalKeyType.TIMESTAMP) {
						try {
							lastReadIncrObj = Date.from(Instant.parse(lastReadIncrKeyValue));
						} catch (Exception e) {
				        	throw new SyncLiteException("Failed to parse the last incremental key value : " + lastReadIncrKeyValue + " for incremental key : " + incrKey + " as Date : " + e.getMessage(), e);
						}
					} else {
						try {
							if (lastReadIncrKeyValue.equals("0001-01-01T00:00:00Z")) {
								//If the key type is not timestamp and we are just starting to replicate 
								//then change the last read value to 0.
								lastReadIncrObj = 0;
							} else {
								lastReadIncrObj = Double.parseDouble(lastReadIncrKeyValue);
							}
						} catch (Exception e) {
			        		throw new SyncLiteException("Failed to parse the last incremental key value : " + lastReadIncrKeyValue + " for incremental key : " + incrKey + " as Double : " + e.getMessage(), e);
						}
					}

					if (includeNullIncrementalKeys) {
						Document incrMinPred1 = new Document(incrKey, new Document("$gt", lastReadIncrObj));
						incrMinPred.append("$or", Arrays.asList(incrMinPred1, incrKeyNullPred));
					} else {
						Document incrMinPred1 = new Document(incrKey, new Document("$gt", lastReadIncrObj));						
						incrMinPred = incrMinPred1;
					}

					String maxIncrKeyValue = maxIncrementalKeyColumnVals.get(incrKey); 
			        Object maxIncrObj = null;
					if (incrKeyType == IncrementalKeyType.TIMESTAMP) {
				        try {
				        	maxIncrObj = Date.from(Instant.parse(maxIncrKeyValue));
				        } catch (Exception e) {
			        		throw new SyncLiteException("Failed to parse the max incremental key value : " + maxIncrKeyValue + " for incremental key : " + incrKey + " as Date : " + e.getMessage(), e);
				        }
					} else {
				        try {
			        		maxIncrObj = Double.parseDouble(maxIncrKeyValue);
				        } catch (Exception e) {
			        		throw new SyncLiteException("Failed to parse the max incremental key value : " + maxIncrKeyValue + " for incremental key : " + incrKey + " as Double : " + e.getMessage(), e);
				        }
					}
					Document incrMaxPred = new Document();
					tracer.info("Incremental key column name : "  + incrKey + ", max value : " + maxIncrKeyValue);

					if (includeNullIncrementalKeys) {
						Document incrMaxPred1 = new Document(incrKey, new Document("$lte", maxIncrObj));
						//incrMaxPred1.append(incrKey, Filters.lte(incrKey, maxIncrDate));
						
						incrMaxPred.append("$or", Arrays.asList(incrMaxPred1, incrKeyNullPred));
					} else {
						//incrMaxPred.append(incrKey, Filters.lte(incrKey, "ISODate(\"" + maxIncrKeyValue + "\")"));
						Document incrMaxPred1 = new Document(incrKey, new Document("$lte", maxIncrObj));
						//incrMaxPred.append(incrKey, Filters.lte(incrKey, maxIncrDate));
						incrMaxPred = incrMaxPred1;
					}
			        orPredicates.add(new Document("$and", Arrays.asList(incrMinPred, incrMaxPred)));
				} else {
					String lastReadIncrKeyValue = srcObject.getLastReadIncrementalKeyColVal(incrKey);
					Object lastReadIncrObj = null;
					if (incrKeyType == IncrementalKeyType.TIMESTAMP) {
						try {
							lastReadIncrObj = Date.from(Instant.parse(lastReadIncrKeyValue));
						} catch (Exception e) {
			        		throw new SyncLiteException("Failed to parse the last incremental key value : " + lastReadIncrKeyValue + " for incremental key : " + incrKey + " as Date : " + e.getMessage(), e);
						}
					} else {
						try {
							if (lastReadIncrKeyValue.equals("0001-01-01T00:00:00Z")) {
								//If the key type is not timestamp and we are just starting to replicate 
								//then change the last read value to 0.
								lastReadIncrObj = 0;
							} else {
								lastReadIncrObj = Double.parseDouble(lastReadIncrKeyValue);
							}
						} catch (Exception e) {
			        		throw new SyncLiteException("Failed to parse the last incremental key value : " + lastReadIncrKeyValue + " for incremental key : " + incrKey + " as Double : " + e.getMessage(), e);
						}
					}
					Document incrMinPred = new Document();
				
					if (includeNullIncrementalKeys) {
						Document incrMinPred1 = new Document(incrKey, new Document("$gt", lastReadIncrObj));
						incrMinPred.append("$or", Arrays.asList(incrMinPred1, incrKeyNullPred));
					} else {
						Document incrMinPred1 = new Document(incrKey, new Document("$gt", lastReadIncrObj));						
						incrMinPred = incrMinPred1;
					}

					orPredicates.add(incrMinPred);
				}
			}
			
			if (!orPredicates.isEmpty()) {
				if (orPredicates.size() == 1) {
					predicate = orPredicates.get(0);
				} else {
					predicate.append("$or", orPredicates);
				}
			}
			
			if (filter != null) {
			    if (!predicate.isEmpty()) {
			        predicate = new Document("$and", Arrays.asList(predicate, filter));
			    } else {
			        predicate = filter;
			    }
			}
			
			long batchRecCount = 0;
			long totalRecCount = 0;
			long batchCount = 1;
			long batchSize = ConfLoader.getInstance().getSrcDBReaderBatchSize();
			tracer.info("Source collection " + srcObject.getName() + " query filter : " + predicate.toJson());
			try(Connection deviceConn = DriverManager.getConnection(syncLiteDeviceURL)) {
				try (PreparedStatement devicePreparedStmt = deviceConn.prepareStatement(srcObject.getInsertTableSql());
						TelemetryStatement deviceStmt = (TelemetryStatement) deviceConn.createStatement()) {

					// Find all documents in the collection
					FindIterable<Document> documents = collection.find(predicate);

					for (Document document : documents) {
						String id = document.getObjectId("_id").toString();
						String doc = valMasker.maskDocument(srcObject, document).toJson();
						devicePreparedStmt.setString(1, id);
						devicePreparedStmt.setString(2, doc);
						
						if (! computeMaxIncrementalKeyValsInDB) {
							//Check if this column is an incremental key column. 
							//If yes then convert it to numeric value (if its type is ts)
							//Maintain current max.
							for (Map.Entry<String, Object> entry : document.entrySet()) {
								String colName = entry.getKey();
								Object val = entry.getValue();
								String formttedVal = null;
								IncrementalKeyType keyType  = incrementalKeyTypes.get(colName.toUpperCase());
								if (keyType != null) {
									//colName is defined as incremental key.
									Double numericColVal = Double.MIN_VALUE;
									if (keyType == IncrementalKeyType.TIMESTAMP) {
										//Try to get epoch in millis for this timestamp field
										//Format the value 
										try {
											formttedVal = sdf.format(val);
											numericColVal = convertTimestampToMills(formttedVal);
										} catch (Exception e) {
											numericColVal = Double.MIN_VALUE;										
										}
									} else {
										try {
											formttedVal = val.toString();
											numericColVal = Double.parseDouble(val.toString());
										} catch (Exception e) {
											numericColVal = Double.MIN_VALUE;
										}
									}

									//Now compare with current max and update max as needed.
									Double currentMax = computedMaxIncrementalKeyColumnVals.get(colName);
									if (currentMax != null) {
										if (currentMax < numericColVal) {
											computedMaxIncrementalKeyColumnVals.put(colName, numericColVal);
											maxIncrementalKeyColumnVals.put(colName, formttedVal);
										}
									}
								}
							}
						}
						devicePreparedStmt.addBatch();
						++batchRecCount;
						++totalRecCount;
						if (batchRecCount == batchSize) {
							tracer.info("Flushing batch number " + batchCount + " for object : " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + ", batch size : " + batchRecCount);
							devicePreparedStmt.executeBatch();

							srcObject.incrLastReadRecordsFetched(batchRecCount);
							Monitor.getInstance().registerChangedObject(srcObject);	

							batchRecCount = 0;
							++batchCount;
						}
					}

					if (batchRecCount > 0) {
						tracer.info("Flushing last batch number : " + batchCount  + " for object : " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + ", batch size : " + batchRecCount);
						devicePreparedStmt.executeBatch();

						srcObject.incrLastReadRecordsFetched(batchRecCount);
						Monitor.getInstance().registerChangedObject(srcObject);	

					}

					if (totalRecCount > 0) {

						//
						//Update all entries.
						//
						for (Map.Entry<String, String> entry : maxIncrementalKeyColumnVals.entrySet()) {						
							deviceStmt.executeUnlogged("UPDATE synclite_dbreader_checkpoint SET column_value = '" + entry.getValue() + "' WHERE object_name = '" + srcObject.getName() + "' AND column_name = '" + entry.getKey() + "'");
							srcObject.setLastReadIncrementalKeyColVal(entry.getKey(), entry.getValue());
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
			this.tracer.error("Failed to read data from MongoDB collection : " + srcObject.getName() + " : " + e.getMessage(), e);
			throw new SyncLiteException("Failed to read data from MongoDB collection : " + srcObject.getName() + " : " + e.getMessage(), e);
		}
	}

	@Override
	protected Double convertTimestampToMills(String val) {
		try {
	        // Define the formatter for ISO dates
	        DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
	        // Parse the ISO date string
	        LocalDateTime dateTime = LocalDateTime.parse(val, formatter);
	        Double ret = (double) dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
	        return ret;
		} catch (Exception e) {    
			try {
				//Try with one format for now.
				//We will make this more robust by handling other possible formats.
				//
				Timestamp ts = Timestamp.valueOf(val);
				Double ret = (double) ts.toInstant().toEpochMilli();
				return ret;
			} catch (Exception e1) {
				//Try parsing as a numeric value 
				
				try {
					Double ret = Double.parseDouble(val);
					return ret;
				} catch (Exception e2) {
					//Ignore
				}
				return Double.MIN_VALUE;
			}
		}
	}

}

