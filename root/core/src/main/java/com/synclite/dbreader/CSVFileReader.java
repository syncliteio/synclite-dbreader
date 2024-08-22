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

import java.io.FileReader;
import java.io.Reader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

import io.synclite.logger.*;

public class CSVFileReader extends DBReader {

	private static CSVFormat csvFormat = initCSVFormat();
	
	//Only one object of fileStorage to be used by all readers	
	private FileStorage fileStorage; 
	
	public CSVFileReader(DBObject object, Logger tracer) throws SyncLiteException {
		super(object, tracer);
		fileStorage = FileStorage.getInstance(tracer);
	}

	private static CSVFormat initCSVFormat() {
		CSVFormat csvFormat = CSVFormat.DEFAULT;
		if (ConfLoader.getInstance().getSrcCsvFilesWithHeaders()) {
			csvFormat = csvFormat.withFirstRecordAsHeader();
		}
		csvFormat = csvFormat.withDelimiter(ConfLoader.getInstance().getSrcCSVFilesFieldDelimiter())
							 .withRecordSeparator (ConfLoader.getInstance().getSrcCSVFilesRecordDelimiter())
							 .withEscape(ConfLoader.getInstance().getSrcCSVFilesEscapeCharacter())
							 .withQuote(ConfLoader.getInstance().getSrcCSVFilesQuoteCharacter())
							 .withNullString(ConfLoader.getInstance().getSrcCSVFilesNullString())
							 .withIgnoreEmptyLines(ConfLoader.getInstance().getSrcCsvFilesIgnoreEmptyLines())
							 .withTrim(ConfLoader.getInstance().getSrcCsvFilesTrimFields());
		return csvFormat;
	}

	@Override
	protected void fullReadKeys() throws SyncLiteException {
		throw new SyncLiteException("Delete-Sync functionality is not supported for CSV source");
	}

	@Override
	protected long fullReadInternal() throws SyncLiteException {
		try {
			String syncLiteDeviceURL = "jdbc:synclite_telemetry:" + srcObject.getDeviceFilePath();
			List<Path> storageDataFiles = new ArrayList<Path>();
			HashMap<Path, Instant> creationTimes = new HashMap<Path, Instant>();
			try {
				fileStorage.listFilesOrderByCreationTime(srcObject.getName(), null, storageDataFiles, creationTimes, tracer);
			} catch (Exception e) {
				throw new SyncLiteException("Failed to get file list from the specified data directory : " + e.getMessage(), e);
			}
			long batchRecCount = 0;
			long totalRecCount = 0;
			long fileRecCount = 0;
			long batchCount = 1;
			long batchSize = ConfLoader.getInstance().getSrcDBReaderBatchSize();
			//Read dataFiles one by one. and publish records.
			try(Connection deviceConn = DriverManager.getConnection(syncLiteDeviceURL)) {
				try (PreparedStatement devicePreparedStmt = deviceConn.prepareStatement(srcObject.getInsertTableSql());
						TelemetryStatement deviceStmt = (TelemetryStatement) deviceConn.createStatement()) {
					for (Path storageDataFile : storageDataFiles) {
						batchRecCount = 0;
						batchCount = 1;
						fileRecCount = 0;
						Path localDataFile = fileStorage.downloadFile(srcObject.getName(), storageDataFile, tracer);
						tracer.info("Starting to read data from file : " + localDataFile);
						
						try (Reader reader = new FileReader(localDataFile.toString());
								CSVParser csvParser = new CSVParser(reader, CSVFileReader.csvFormat)) 
						{
							for (CSVRecord csvRecord : csvParser) {
								int i = 1;
								for (String val : csvRecord) {
									if (i > srcObject.getAllowedColumnCount()) {
										break;
									}
									if (val == null) {
										devicePreparedStmt.setNull(i, Types.VARCHAR);
									} else {
										devicePreparedStmt.setString(i, val);
									}
									++i;
								}

								devicePreparedStmt.addBatch();
								++batchRecCount;
								++fileRecCount;
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

						}
						if (batchRecCount > 0) {
							tracer.info("Flushing last batch number : " + batchCount  + " for object : " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + ", batch size : " + batchRecCount);
							devicePreparedStmt.executeBatch();

							srcObject.incrLastReadRecordsFetched(batchRecCount);
							Monitor.getInstance().registerChangedObject(srcObject);
						}
						tracer.info("Finished reading data from file : " + localDataFile + ", file record count : " + fileRecCount);
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
			this.tracer.error("Failed to read data from data files : " + e.getMessage(), e);
			throw new SyncLiteException("Failed to read data from data files : " + e.getMessage(), e);
		}
	}

	@Override
	protected long incrementalReadInternal() throws SyncLiteException {
		try {
			String syncLiteDeviceURL = "jdbc:synclite_telemetry:" + srcObject.getDeviceFilePath();
			List<Path> storageDataFiles = new ArrayList<Path>();
			HashMap<Path, Instant> creationTimes = new HashMap<Path, Instant>();
			Path latestFile = null;
			Instant latestFileTS = null;
			String incrKey = srcObject.getIncrementalKeyColumns().get(0);
			String lastSyncedFileTS = srcObject.getLastReadIncrementalKeyColVal(incrKey);	
			Instant lastSyncedFileTSInstant = Instant.parse(lastSyncedFileTS);
			try {
				fileStorage.listFilesOrderByCreationTime(srcObject.getName(), lastSyncedFileTSInstant, storageDataFiles, creationTimes, tracer);
				if (storageDataFiles.isEmpty()) {
					this.tracer.info("No new data files found for object : " + srcObject.getFullName());
					return 0;
				}
			} catch (Exception e) {
				throw new SyncLiteException("Failed to get file list from the specified data directory : " + e.getMessage(), e);
			}
			long batchRecCount = 0;
			long totalRecCount = 0;
			long fileRecCount = 0;
			long batchCount = 1;
			long batchSize = ConfLoader.getInstance().getSrcDBReaderBatchSize();
			//Read dataFiles one by one. and publish records.
			try(Connection deviceConn = DriverManager.getConnection(syncLiteDeviceURL)) {
				try (PreparedStatement devicePreparedStmt = deviceConn.prepareStatement(srcObject.getInsertTableSql());
						TelemetryStatement deviceStmt = (TelemetryStatement) deviceConn.createStatement()) {
					for (Path storageDataFile : storageDataFiles) {
						fileRecCount = 0;
						batchRecCount = 0;
						batchCount = 1;
						
						Path localDataFile = fileStorage.downloadFile(srcObject.getName(), storageDataFile, tracer);
						
						Instant dataFileCreationTime = creationTimes.get(storageDataFile);

						tracer.info("Starting to read data from file : " + localDataFile);
						try (Reader reader = new FileReader(localDataFile.toString());
								CSVParser csvParser = new CSVParser(reader, CSVFileReader.csvFormat)) 
						{
							for (CSVRecord csvRecord : csvParser) {
								int i = 1;
								for (String val : csvRecord) {
									if (i > srcObject.getAllowedColumnCount()) {
										break;
									}
									if (val == null) {
										devicePreparedStmt.setNull(i, Types.VARCHAR);
									} else {
										devicePreparedStmt.setString(i, val);
									}
									++i;
								}

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
						if (batchRecCount > 0) {
							tracer.info("Flushing last batch number : " + batchCount  + " for object : " + srcObject.getFullName() + ", partition : " + srcObject.getPartitionIdx() + ", batch size : " + batchRecCount);
							devicePreparedStmt.executeBatch();

							srcObject.incrLastReadRecordsFetched(batchRecCount);
							Monitor.getInstance().registerChangedObject(srcObject);	

						}

						deviceStmt.executeUnlogged("UPDATE synclite_dbreader_checkpoint SET column_value = '" + dataFileCreationTime + "' WHERE object_name = '" + srcObject.getName() + "' AND column_name = '" + incrKey + "'");
						srcObject.setLastReadIncrementalKeyColVal(incrKey, dataFileCreationTime.toString());
						
						tracer.info("Finished reading data from file : " + localDataFile + ", file record count : " + fileRecCount);
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
			this.tracer.error("Failed to read data from data files : " + e.getMessage(), e);
			throw new SyncLiteException("Failed to read data from data files : " + e.getMessage(), e);
		}
	}

}

