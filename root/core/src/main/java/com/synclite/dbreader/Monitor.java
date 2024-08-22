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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;


public class Monitor {

	private static Monitor INSTANCE;
	public static final Monitor getInstance() {
		return INSTANCE;
	}
	
	public static synchronized Monitor createAndGetInstance(Logger tracer) {		
		if (INSTANCE != null) {
			return INSTANCE;
		} else {
			INSTANCE = new Monitor(tracer);
		}
		return INSTANCE;
	}

	private abstract class Dumper {
		public void run() {
			while (!Thread.interrupted()) {
				try {            	
					dump();
					Thread.sleep(screenRefreshIntervalMs);
				} catch (InterruptedException e) {
					Thread.interrupted();
				}
			}
		}

		protected abstract void dump();

		protected abstract void init() throws SyncLiteException;

		protected abstract void schedule();
		protected abstract void shutdown();
		
		protected ScheduledExecutorService monitorService;
	}

	private class FileDumper extends Dumper{
		Connection statsConn;
		PreparedStatement updateDashboardPstmt;
		PreparedStatement deleteTableStatusPstmt;
		PreparedStatement insertTableStatusPstmt;

		private String createDashboardTableSql = "CREATE TABLE if not exists dashboard(header TEXT, total_objects LONG, failed_objects LONG, snapshot_objects LONG, incremental_objects LONG, last_read_start_time LONG, last_read_end_time LONG, last_delete_sync_start_time LONG, last_delete_sync_end_time LONG, total_records_fetched LONG, last_heartbeat_time, last_job_start_time LONG)";
		private String insertDashboardTableSql = "INSERT INTO dashboard (header, total_objects, failed_objects, snapshot_objects, incremental_objects, last_read_start_time, last_read_end_time, last_delete_sync_start_time, last_delete_sync_end_time, total_records_fetched, last_heartbeat_time, last_job_start_time) VALUES('$1', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)";
		private String updateDashboardTableSql = "UPDATE dashboard SET total_objects = ?, failed_objects = ?, snapshot_objects = ? , incremental_objects = ?, last_read_start_time = ?, last_read_end_time = ?, last_delete_sync_start_time = ?, last_delete_sync_end_time = ?, total_records_fetched = ?, last_heartbeat_time = ?, last_job_start_time = ?";
		private String selectDashboardTableSql = "SELECT last_read_start_time, last_read_end_time, last_delete_sync_start_time, last_delete_sync_end_time, total_records_fetched FROM dashboard;";

		private String createTableStatisticsSql = "CREATE TABLE IF NOT EXISTS object_statistics(object TEXT, read_type TEXT, last_read_start_time LONG, last_read_end_time LONG, last_read_records_fetched LONG, last_read_incremental_key_column_values TEXT, last_delete_sync_start_time LONG, last_delete_sync_end_time LONG, last_delete_sync_keys_fetched LONG, last_read_status TEXT, last_read_status_description TEXT, total_records_fetched LONG, total_inferred_ddls LONG, total_full_reload_count LONG, total_schema_reload_count LONG, last_published_commit_id LONG, PRIMARY KEY(object))";
		private String insertTableStatisticsSql = "INSERT INTO object_statistics(object, read_type, last_read_start_time, last_read_end_time, last_read_records_fetched, last_read_incremental_key_column_values, last_delete_sync_start_time, last_delete_sync_end_time, last_delete_sync_keys_fetched, last_read_status, last_read_status_description, total_records_fetched, total_inferred_ddls, total_full_reload_count, total_schema_reload_count, last_published_commit_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		private String deleteTableStatisticsSql = "DELETE FROM object_statistics WHERE object = ?";
		private String selectTableStatisticsSql = "SELECT object, last_read_start_time, last_read_end_time, last_read_records_fetched, last_delete_sync_start_time, last_delete_sync_end_time, last_delete_sync_keys_fetched, last_read_status, last_read_status_description, total_records_fetched, total_inferred_ddls, total_full_reload_count, total_schema_reload_count, last_published_commit_id FROM object_statistics";

		private FileDumper() {
		}

		private final void initStats() throws SyncLiteException {
			String url = "jdbc:sqlite:" + Path.of(ConfLoader.getInstance().getSyncLiteDeviceDir().toString(), "synclite_dbreader_statistics.db").toString();
			try {
				statsConn = DriverManager.getConnection(url);

				try (Statement stmt = statsConn.createStatement()) {
					stmt.execute(createDashboardTableSql);
					try (ResultSet rs = stmt.executeQuery(selectDashboardTableSql)) {
						if (!rs.next()) {
							insertDashboardTableSql = insertDashboardTableSql.replace("$1", Monitor.this.header);
							stmt.execute(insertDashboardTableSql);
						} else {
							setLastReadEndTime(rs.getLong("last_read_start_time"));
							setLastReadEndTime(rs.getLong("last_read_end_time"));
							setLastReadEndTime(rs.getLong("last_delete_sync_start_time"));
							setLastReadEndTime(rs.getLong("last_delete_sync_end_time"));
							setTotalRecordsFetched(rs.getLong("total_records_fetched"));
						}
					}	

					//Add a new column last_job_start_time to dashboard table if not present.
					String checkColSql = "PRAGMA table_info('dashboard')";
		            try (ResultSet rs = stmt.executeQuery(checkColSql)) {
		                boolean lastJobStartTimeExists = false;
		                while (rs.next()) {
		                    String name = rs.getString("name");
		                    if (name.equals("last_job_start_time")) {
		                    	lastJobStartTimeExists = true;
		                        break;
		                    }
		                }		                
		                if (!lastJobStartTimeExists) {
			                String addColSql = "ALTER TABLE dashboard ADD COLUMN last_job_start_time LONG DEFAULT(0)";
		                	stmt.execute(addColSql);
		                }
		            }

					updateDashboardPstmt = statsConn.prepareStatement(updateDashboardTableSql);        			
					
					stmt.execute(createTableStatisticsSql);
					
					//Upgrade code path
				
					//Add a new column last_published_commit_id to object_statistics table if not present.
					checkColSql = "PRAGMA table_info('object_statistics')";
		            try (ResultSet rs = stmt.executeQuery(checkColSql)) {
		                boolean lastPublishedCommitIDExists = false;
		                while (rs.next()) {
		                    String name = rs.getString("name");
		                    if (name.equals("last_published_commit_id")) {
		                    	lastPublishedCommitIDExists = true;
		                        break;
		                    }
		                }                
		                if (!lastPublishedCommitIDExists) {
			                String addColSql = "ALTER TABLE object_statistics ADD COLUMN last_published_commit_id LONG DEFAULT(0)";
		                	stmt.execute(addColSql);
		                }
		            }

					ConcurrentHashMap<String, DBObject> dbObjects = DBReaderDriver.getInstance().getDBObjects();
					//Read table statistics and set them in respective DBObjects if present.
					try (ResultSet tsRS = stmt.executeQuery(selectTableStatisticsSql)) {
						while(tsRS.next()) {
							String objectName = tsRS.getString("object");
							DBObject dbObject = dbObjects.get(objectName); 
							if (dbObject != null) {
								//last_read_start_time, last_read_end_time, last_read_records_fetched, last_delete_sync_start_time, last_delete_sync_end_time, last_delete_sync_keys_fetched, total_records_fetched, total_inferred_ddls, total_full_reload_count, total_schema_reload_count, last_read_status, last_read_status_description FROM object_statistics
								dbObject.setLastReadStartTime(tsRS.getLong("last_read_start_time"));
								dbObject.setLastReadEndTime(tsRS.getLong("last_read_end_time"));
								dbObject.setLastReadRecordsFetched(tsRS.getLong("last_read_records_fetched"));
								dbObject.setLastDeleteSyncStartTime(tsRS.getLong("last_delete_sync_start_time"));
								dbObject.setLastDeleteSyncStartTime(tsRS.getLong("last_delete_sync_end_time"));
								dbObject.setLastDeleteSyncKeysFetched(tsRS.getLong("last_delete_sync_keys_fetched"));
								dbObject.setTotalRecordsFetched(tsRS.getLong("total_records_fetched"));
								dbObject.setTotalInferredDDLs(tsRS.getLong("total_inferred_ddls"));
								dbObject.setTotalFullReloadCount(tsRS.getLong("total_full_reload_count"));
								dbObject.setTotalSchemaReloadCount(tsRS.getLong("total_schema_reload_count"));
								dbObject.setLastReadStatus(tsRS.getString("last_read_status"));
								dbObject.setLastReadStatus(tsRS.getString("last_read_status_description"));
								dbObject.setLastPublishedCommitID(tsRS.getLong("last_published_commit_id"));
							}
						}
					}					
					deleteTableStatusPstmt = statsConn.prepareStatement(deleteTableStatisticsSql);
					insertTableStatusPstmt = statsConn.prepareStatement(insertTableStatisticsSql);
				}        		

			} catch (SQLException e) {
				tracer.error("Failed to create/open SyncLite DBReader statistics file at : " + url + " : " + e.getMessage() , e);
				throw new SyncLiteException("Failed to create/open SyncLite DBReader statistics file at : " + url, e);
			}

		}

		@Override
		protected void dump() {
			try {
				//screenDumper.dump();
				long currentTime = System.currentTimeMillis();
				if ((lastStatChangeTime < lastStatFlushTime) && ((currentTime - lastStatFlushTime) < heartbeatIntervalMs)) {
					//Skip the update if there is nothing to be updated
					//However force an update if last update was done 30 seconds back
					//as this update also serves as a heartbeat of the consolidator job
					return;
				}				
				statsConn.setAutoCommit(false);
				updateDashboardPstmt.setLong(1, totalObjects.get());
				updateDashboardPstmt.setLong(2, failedObjects.get());
				updateDashboardPstmt.setLong(3, snapshotObjects.get());
				updateDashboardPstmt.setLong(4, incrementalObjects.get());
				updateDashboardPstmt.setLong(5, lastReadStartTime.get());
				updateDashboardPstmt.setLong(6, lastReadEndTime.get());
				updateDashboardPstmt.setLong(7, lastDeleteSyncStartTime.get());
				updateDashboardPstmt.setLong(8, lastDeleteSyncEndTime.get());
				updateDashboardPstmt.setLong(9, totalRecordsFetched.get());
				updateDashboardPstmt.setLong(10, currentTime);
				updateDashboardPstmt.setLong(11, Main.jobStartTime);
				updateDashboardPstmt.addBatch();
				updateDashboardPstmt.executeBatch();

				if (changedObjects.size() > 0) {
					DBObject object = changedObjects.poll();
					while (object != null) {
						deleteTableStatusPstmt.setString(1, object.getName());						
						deleteTableStatusPstmt.execute();
						
						insertTableStatusPstmt.setString(1, object.getName());
						insertTableStatusPstmt.setString(2, object.getReadType());
						insertTableStatusPstmt.setLong(3, object.getLastReadStartTime());
						insertTableStatusPstmt.setLong(4, object.getLastReadEndTime());
						insertTableStatusPstmt.setLong(5, object.getLastReadRecordsFetched());					
						insertTableStatusPstmt.setString(6, object.getLastReadIncrementalKeyColValues());
						insertTableStatusPstmt.setLong(7, object.getLastDeleteSyncStartTime());					
						insertTableStatusPstmt.setLong(8, object.getLastDeleteSyncEndTime());
						insertTableStatusPstmt.setLong(9, object.getLastDeleteSyncKeysFetched());						
						insertTableStatusPstmt.setString(10, object.getLastReadStatus());
						insertTableStatusPstmt.setString(11, object.getLastReadStatusDescription());
						insertTableStatusPstmt.setLong(12, object.getTotalRecordsFetched());						
						insertTableStatusPstmt.setLong(13, object.getTotalInferredDDLs());
						insertTableStatusPstmt.setLong(14, object.getTotalFullReloadCount());
						insertTableStatusPstmt.setLong(15, object.getTotalSchemaReloadCount());
						insertTableStatusPstmt.setLong(16, object.getLastPublishedCommitID());
						insertTableStatusPstmt.execute();
						object = changedObjects.poll();
					}
				}
				statsConn.commit();
				statsConn.setAutoCommit(true);
				lastStatFlushTime = currentTime;
				//TODO keep doing vacuum on statistics file every periodically. 

			} catch (SQLException e) {
				tracer.error("SyncLite dbreader statistics dumper failed with exception : ", e);
			}
		}

		@Override
		protected void init() throws SyncLiteException {
			initStats();
		}

		@Override
		protected void schedule() {
	        monitorService = Executors.newScheduledThreadPool(1);
	        monitorService.scheduleAtFixedRate(this::dump, 0, screenRefreshIntervalMs, TimeUnit.MILLISECONDS);			
			//start();
		}

		@Override
		protected void shutdown() {
			try {
				if (this.monitorService != null) {
					monitorService.shutdownNow();
				}
			} catch (Exception e) {
				//Ignore
			}
		}
	}

	private String header;
	private AtomicLong totalObjects= new AtomicLong(0);
	private AtomicLong failedObjects = new AtomicLong(0);
	private AtomicLong snapshotObjects = new AtomicLong(0);
	private AtomicLong incrementalObjects = new AtomicLong(0);
	private AtomicLong lastReadStartTime = new AtomicLong(0);
	private AtomicLong lastReadEndTime = new AtomicLong(0);
	private AtomicLong lastDeleteSyncStartTime = new AtomicLong(0);
	private AtomicLong lastDeleteSyncEndTime = new AtomicLong(0);
	private AtomicLong totalRecordsFetched = new AtomicLong(0);
	private static long screenRefreshIntervalMs = 1000;
	private static final long heartbeatIntervalMs = 30000;  
	private volatile long lastStatChangeTime = System.currentTimeMillis();
	private long lastStatFlushTime = System.currentTimeMillis();
	private Dumper dumper;
	private List<Dumper>additionalDumpers = new ArrayList<Dumper>();
	private BlockingQueue<DBObject> changedObjects = new LinkedBlockingQueue<DBObject>();
	private Logger tracer;
	private boolean monitorEnabled;

	private Monitor(Logger tracer) {
		this.tracer = tracer;
		this.header = "Reading tables from " + ConfLoader.getInstance().getSrcName() + " into directory : " + ConfLoader.getInstance().getSyncLiteDeviceDir();
		dumper = new FileDumper();

		screenRefreshIntervalMs = ConfLoader.getInstance().getUpdateStatisticsIntervalS() * 1000;
		monitorEnabled = ConfLoader.getInstance().getEnableStatisticsCollector();
	}

	public final void registerChangedObject(DBObject d) {
		try {
			//Enqueue only if Monitor is enabled otherwise there is no one to dequeue.
			//
			if (monitorEnabled) {
				changedObjects.put(d);
				this.lastStatChangeTime = System.currentTimeMillis();
			}
		} catch (InterruptedException e) {
			Thread.interrupted();
		}
	}

	public long getTotalTables() {
		return this.totalObjects.longValue();
	}
	
	public void setTotalObjects(long totalTables) {
		this.totalObjects.set(totalTables);
	}

	public void incrTotalTables(long cnt) {
		this.totalObjects.addAndGet(cnt);
	}

	public long getFailedObjects() {
		return this.totalObjects.longValue();
	}
	
	public void setFailedObjects(long cnt) {
		this.failedObjects.set(cnt);
		this.lastStatChangeTime = System.currentTimeMillis();
	}

	public void incrFailedTables(long cnt) {
		this.failedObjects.addAndGet(cnt);
	}

	public long getSnapshotObjectCount() {
		return this.snapshotObjects.longValue();
	}
	
	public void setSnapshotObjectCount(long cnt) {
		this.snapshotObjects.set(cnt);
	}

	public void incrSnapshotObjects(long cnt) {
		this.snapshotObjects.addAndGet(cnt);
	}

	public long getIncrementalObjectCount() {
		return this.incrementalObjects.longValue();
	}
	
	public void setIncrementalObjectCount(long cnt) {
		this.incrementalObjects.set(cnt);
	}

	public void incrIncrementalObjects(long cnt) {
		this.incrementalObjects.addAndGet(cnt);
	}

	public long getLastReadStartTime() {
		return this.lastReadStartTime.longValue();
	}
	
	public void setLastReadStartTime(long t) {
		this.lastReadStartTime.set(t);
	}

	public long getLastReadEndTime() {
		return this.lastReadEndTime.longValue();
	}
	
	public void setLastReadEndTime(long t) {
		this.lastReadEndTime.set(t);
	}

	public long getLastDeleteSyncStartTime() {
		return this.lastDeleteSyncStartTime.longValue();
	}
	
	public void setLastDeleteSyncStartTime(long t) {
		this.lastDeleteSyncStartTime.set(t);
	}

	public long getLastDeleteSyncEndTime() {
		return this.lastDeleteSyncEndTime.longValue();
	}
	
	public void setLastDeleteSyncEndTime(long t) {
		this.lastDeleteSyncEndTime.set(t);
	}


	public long getTotalRecordsFetched() {
		return this.totalRecordsFetched.longValue();
	}
	
	public void setTotalRecordsFetched(long cnt) {
		this.totalRecordsFetched.set(cnt);
	}

	public void incrTotalRecordsFetched(long cnt) {
		this.totalRecordsFetched.addAndGet(cnt);
	}


	public void start() throws SyncLiteException {
		try {
			dumper.init();
			dumper.schedule();

			for (Dumper d : additionalDumpers) {
				d.init();
				d.schedule();
			}
		} catch (Exception e) {
			tracer.error("Failed to start statistics collector : " + e.getMessage(), e);
			//dumper.interrupt();
			dumper.shutdown();
		}
	}

	public final void deleteTableFromStats(DBObject t) throws SyncLiteException {
		Path statsFilePath = Path.of(ConfLoader.getInstance().getSyncLiteDeviceDir().toString(), "synclite_dbreader_statistics.db");
		String url = "jdbc:sqlite:" + statsFilePath;
		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.execute("DELETE FROM object_statistics where object = '" + t.getName() + "'");
			}
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to delete object : " + t + " from dbreader statistics file");		 
		}
	}

	public void incrSnapshotObjectCount() {
		this.snapshotObjects.addAndGet(1);
	}
}
