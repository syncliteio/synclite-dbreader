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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.json.JSONTokener;

import io.synclite.logger.*;


/*import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
 */


public class DBReaderDriver implements Runnable{

	private class DBObjectGroup {
		HashMap<Integer, DBObject> objects;

		private Lock processingLock = new ReentrantLock();
		private boolean aquireProcessingLock() {
			return processingLock.tryLock();
		}

		private void releaseProcessingLock() {
			try {
				processingLock.unlock();
			} catch (Exception e) {
				//Ignore
			}
		}
	}
	
	private static final class InstanceHolder {
		private static DBReaderDriver INSTANCE = new DBReaderDriver();
	}

	public static DBReaderDriver getInstance() {
		return InstanceHolder.INSTANCE;
	}

	private Logger globalTracer;
	private ConcurrentHashMap<String, DBObject> dbObjects = new ConcurrentHashMap<String, DBObject>();
	private ConcurrentHashMap<String, DBObject> failedDBObjects = new ConcurrentHashMap<String, DBObject>();
	private ConcurrentHashMap<DBObject, DBObjectGroup> failedDBObjectGroups = new ConcurrentHashMap<DBObject, DBObjectGroup>();

	private ConcurrentHashMap<String, DBObjectGroup> objectGroups = new ConcurrentHashMap<String, DBObjectGroup>();

	// private HashMap<Device, List<>>
	private final BlockingQueue<DBObjectGroup> tasks = new LinkedBlockingQueue<DBObjectGroup>(Integer.MAX_VALUE);
	private ScheduledExecutorService newObjectDetector;
	private ScheduledExecutorService objectScheduler;
	private ScheduledExecutorService failedObjectScheduler;
	private List<ExecutorService> readerTasks;
	private Path dbReaderMetadataFile;

	//ScheduledExecutorService externalCommandLoader;

	private DBReaderDriver() {
	}
	private final void initLogger() {
		this.globalTracer = Logger.getLogger(DBReaderDriver.class);    	
		switch (ConfLoader.getInstance().getTraceLevel()) {
		case ERROR:
			globalTracer.setLevel(Level.ERROR);
			break;
		case INFO:
			globalTracer.setLevel(Level.INFO);
			break;
		case DEBUG:
			globalTracer.setLevel(Level.DEBUG);
			break;
		}
		RollingFileAppender fa = new RollingFileAppender();
		fa.setName("FileLogger");
		fa.setFile(Path.of(ConfLoader.getInstance().getSyncLiteDeviceDir().toString(), "synclite_dbreader.trace").toString());
		fa.setLayout(new PatternLayout("%d %-5p [%t] %m%n"));
		fa.setMaxBackupIndex(10);
		fa.setMaxFileSize("10MB"); // Set the maximum file size to 10 MB
		fa.setAppend(true);
		fa.activateOptions();
		globalTracer.addAppender(fa);    	
	}

	public final Logger getTracer() {
		return this.globalTracer;
	}

	public final void stopSyncServices() {
		//try {

		if (failedObjectScheduler != null) {
			failedObjectScheduler.shutdownNow();
		}

		if (objectScheduler != null) {
			objectScheduler.shutdownNow();
		}
		//} 
		//catch (InterruptedException e) {
		//	Thread.interrupted();
		//}
	}

	private final void runReadServices() throws SyncLiteException {
		try {
			
			//if Infer create object is turned ON then schedule objectDetector
			
			if (ConfLoader.getInstance().getSrcInferObjectCreate()) {
				newObjectDetector = Executors.newScheduledThreadPool(1);
				newObjectDetector.scheduleWithFixedDelay(this::detectNewObjects, 0, ConfLoader.getInstance().getSrcDBReaderIntervalS(), TimeUnit.SECONDS);
			}
			
			objectScheduler = Executors.newScheduledThreadPool(1);
			objectScheduler.scheduleWithFixedDelay(this::scheduleObjects, 0, ConfLoader.getInstance().getSrcDBReaderIntervalS(), TimeUnit.SECONDS);

			if (ConfLoader.getInstance().getRetryFailedObjects()) {
				failedObjectScheduler = Executors.newScheduledThreadPool(1);
				failedObjectScheduler.scheduleWithFixedDelay(this::scheduleFailedObjectGroups, 0, ConfLoader.getInstance().getFailedObjectRetryIntervalS(), TimeUnit.SECONDS);
			}

			readerTasks = new ArrayList<ExecutorService>(ConfLoader.getInstance().getSrcDBReaderProcessors());			
			for (int i = 0; i < ConfLoader.getInstance().getSrcDBReaderProcessors(); ++i) {
				ScheduledExecutorService readerTask = Executors.newScheduledThreadPool(1);
				readerTasks.add(readerTask);
				readerTask.scheduleWithFixedDelay(this::doRead, 0, 1, TimeUnit.NANOSECONDS);
			}
			for (ExecutorService syncer : readerTasks) {
				syncer.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
			}
		} catch (InterruptedException e) {
			Thread.interrupted();
		}
	}


	private final void runReadServicesOnce() throws SyncLiteException {
		try {
			Monitor.getInstance().setLastReadStartTime(System.currentTimeMillis());
			scheduleObjects();
			ExecutorService readerTask = Executors.newFixedThreadPool(ConfLoader.getInstance().getSrcDBReaderProcessors());
			List<Future<?>> futures = new ArrayList<>();

			for (int i = 0; i < ConfLoader.getInstance().getSrcDBReaderProcessors(); ++i) {
				Future<?> future = readerTask.submit(this::doReadOnce);
				futures.add(future);
			}

			// Wait for all futures to complete
			for (Future<?> future : futures) {
				try {
					future.get(); // This blocks until the task is completed
				} catch (InterruptedException e) {
					Thread.interrupted();
				} catch (ExecutionException e) {
					throw new SyncLiteException(e);
				}
			}

			// Shutdown the reader task
			readerTask.shutdown();
			
			createSystemDeviceSendShutdown();
			Monitor.getInstance().setLastReadEndTime(System.currentTimeMillis());
			//Wait for 5 seconds for monitor to update statistics.
			Thread.sleep(5000);
			//
		} catch (Exception e) {
			throw new SyncLiteException("Failed to run single iteration read job : ", e);
		}
	}

	private final void runDeleteSyncServices() throws SyncLiteException {
		try {
			Monitor.getInstance().setLastDeleteSyncStartTime(System.currentTimeMillis());
			scheduleObjects();
			ExecutorService readerTask = Executors.newFixedThreadPool(ConfLoader.getInstance().getSrcDBReaderProcessors());
			List<Future<?>> futures = new ArrayList<>();

			for (int i = 0; i < ConfLoader.getInstance().getSrcDBReaderProcessors(); ++i) {
				Future<?> future = readerTask.submit(this::doDeleteSync);
				futures.add(future);
			}

			// Wait for all futures to complete
			for (Future<?> future : futures) {
				try {
					future.get(); // This blocks until the task is completed
				} catch (InterruptedException e) {
					Thread.interrupted();
				} catch (ExecutionException e) {
					throw new SyncLiteException(e);
				}
			}

			// Shutdown the reader task
			readerTask.shutdown();
			Monitor.getInstance().setLastDeleteSyncEndTime(System.currentTimeMillis());
			//Wait for 5 seconds for monitor to update statistics.
			Thread.sleep(5000);
			//
		} catch (Exception e) {
			throw new SyncLiteException("Failed to run delete sync job : ", e);
		}
	}

	/*
	private final void runDeleteSyncServices() throws SyncLiteException {
		try {
			scheduleTables();	
			readerTasks = new ArrayList<ExecutorService>(ConfLoader.getInstance().getSrcDBReaderProcessors());
			List<Future<?>> futures = new ArrayList<>();

			for (int i = 0; i < ConfLoader.getInstance().getSrcDBReaderProcessors(); ++i) {
				ExecutorService readerTask = Executors.newFixedThreadPool(1);
				readerTasks.add(readerTask);
				Future<?> future = readerTask.submit(this::doDeleteSync);
				futures.add(future);
			}

			for (Future<?> future : futures) {
				try {
					future.get(); // This blocks until the task is completed
				} catch (InterruptedException e) {
					Thread.interrupted();
				} catch(ExecutionException e) {
					throw new SyncLiteException(e);
				}
			}


			// Shutdown the reader tasks
			for (ExecutorService reader : readerTasks) {
				reader.shutdown();
			}

		} catch (Exception e) {
			throw new SyncLiteException("Failed to run delete sync job : ", e);
		}
	}
	 */

	private final void doRead() {
		DBObjectGroup objectGrp = null;
		try {
			objectGrp = tasks.take();
			if (objectGrp == null) {
				return;
			}		
			if (!objectGrp.aquireProcessingLock()) {
				//If the device is already being processed, then add it to queue and move on
				addObjectGroupTask(objectGrp);
				return;
			}
			DBObject object = null;
			for (int idx=1; idx <= objectGrp.objects.size() ; ++idx) {
				try {
					object = objectGrp.objects.get(idx);
					DBReader reader = DBReader.getInstance(object, globalTracer);
					reader.processObject();
					
					//Success
					//Remove from failed object if present and update count
					failedDBObjects.remove(object.getName());
					failedDBObjectGroups.remove(object);
					Monitor.getInstance().setFailedObjects(failedDBObjects.size());
					//
				} catch(Exception e) {
					globalTracer.error("Reading for object " + object.getName() + " belonging to group/device "  + object.getDeviceName() + " failed with exception : " + e.getMessage(), e);
					//Add to failed objects
					failedDBObjects.put(object.getName(), object);
					failedDBObjectGroups.put(object, objectGrp);
					Monitor.getInstance().setFailedObjects(failedDBObjects.size());
					break;
				} 
			}
		} catch (InterruptedException e) {
			Thread.interrupted();
		} finally {
			if (objectGrp != null) {
				objectGrp.releaseProcessingLock();
			}
		}
	}

	private final void doReadOnce() {
		do {
			DBObjectGroup objectGrp = null;
			try {
				objectGrp = tasks.poll();
				if (objectGrp == null) {
					return;
				}		
				if (!objectGrp.aquireProcessingLock()) {
					//If the device is already being processed, then add it to queue and move on
					addObjectGroupTask(objectGrp);
					continue;
				}
				DBObject object = null;
				for (int idx=1; idx <= objectGrp.objects.size() ; ++idx) {
					try {
						object = objectGrp.objects.get(idx);
						DBReader reader = DBReader.getInstance(object, globalTracer);                    
						reader.processObject();
					} catch(Exception e) {
						globalTracer.error("Read once for object " + object.getName() + " belonging to group/device "  + object.getDeviceName() + " failed with exception : " + e.getMessage(), e);
						break;
					} 
				}
			} catch (InterruptedException e) {
				Thread.interrupted();
			} finally {
				if (objectGrp != null) {
					objectGrp.releaseProcessingLock();
				}
			}
		} while (!tasks.isEmpty());
	}

	private final void doDeleteSync() {
		do {
			DBObjectGroup objectGrp = null;
			try {
				objectGrp = tasks.poll();
				if (objectGrp == null) {
					return;
				}		
				if (!objectGrp.aquireProcessingLock()) {
					//If the device is already being processed, then add it to queue and move on
					addObjectGroupTask(objectGrp);
					continue;
				}
				DBObject object = null;
				for (int idx=1; idx <= objectGrp.objects.size() ; ++idx) {
					try {
						object = objectGrp.objects.get(idx);
						DBReader reader = DBReader.getInstance(object, globalTracer);                    
						reader.processDeleteSync();
					} catch(Exception e) {
						globalTracer.error("Delete Sync for object " + object.getName() + " belonging to group/device "  + object.getDeviceName() + " failed with exception : " + e.getMessage(), e);
						break;
					} 
				}
			} catch (InterruptedException e) {
				Thread.interrupted();
			} finally {
				if (objectGrp != null) {
					objectGrp.releaseProcessingLock();
				}
			}
		} while (!tasks.isEmpty());
	}

	
	private final synchronized void addObjectGroupTask(DBObjectGroup grp) throws InterruptedException {
		tasks.put(grp);
	}


	private final void initDriver() throws SyncLiteException {
		initLogger();
		initDBMetadataRedader();
		createMonitor();
		initSourceDB();
		loadObjects();
		startMonitor();
		initSyncLiteDevices();
	}
	
	private final void initDBMetadataRedader() {
		DBMetadataReader.setLogger(this.globalTracer);
	}
	
	private void createMonitor() throws SyncLiteException {
		Monitor.createAndGetInstance(this.globalTracer);
	}
	
	private void startMonitor() throws SyncLiteException {
		Monitor m = Monitor.getInstance();
		if (ConfLoader.getInstance().getEnableStatisticsCollector()) {
			m.start();
		}
	}
	
	private void loadObjects() throws SyncLiteException {
		//Load tables/views from metadata file
		try {
			long snapshotObjectCount = 0;
			long incrementalObjectCount = 0;
			this.globalTracer.info("Loading tables/views from metadata");
			this.dbReaderMetadataFile = ConfLoader.getInstance().getSyncLiteDeviceDir().resolve("synclite_dbreader_metadata.db");
			String url = "jdbc:sqlite:" + dbReaderMetadataFile;
			Class.forName("org.sqlite.JDBC");
			try (Connection conn = DriverManager.getConnection(url)) {
				try(Statement stmt = conn.createStatement()) {
					DBObject o = null;
					try (ResultSet rs = stmt.executeQuery("SELECT object_name, allowed_columns, unique_key_columns, incremental_key_columns, group_name, group_position, mask_columns, delete_condition, select_conditions FROM src_object_info WHERE enable = 1")) {
						while (rs.next()) {
							if (Main.CMD == CMDType.READ) {
								String selectConditions = rs.getString("select_conditions");
								HashMap<Integer, String> conditions = null;
								if ((selectConditions != null) && (!selectConditions.isBlank())) {
									//Create 1 Object object for each specified selectCondition									
									conditions = getAllSelectConditions(rs.getString("object_name"), selectConditions);
									for (Map.Entry<Integer, String> entry : conditions.entrySet()) {										
										o = new DBObject(this.globalTracer, rs.getString("object_name"), rs.getString("allowed_columns"), rs.getString("unique_key_columns"), rs.getString("incremental_key_columns"), rs.getString("group_name"), rs.getInt("group_position"), rs.getString("mask_columns"), rs.getString("delete_condition"), entry.getKey(), selectConditions, conditions);
										dbObjects.put(rs.getString("object_name"), o);
										addToObjectGroup(o);
									}
								} else {
									o = new DBObject(this.globalTracer, rs.getString("object_name"), rs.getString("allowed_columns"), rs.getString("unique_key_columns"), rs.getString("incremental_key_columns"), rs.getString("group_name"), rs.getInt("group_position"), rs.getString("mask_columns"), rs.getString("delete_condition"), 0, selectConditions, conditions);
									dbObjects.put(rs.getString("object_name"), o);
									addToObjectGroup(o);
								}
							} else if (Main.CMD == CMDType.DELETE_SYNC){
								HashMap<Integer, String> conditions = null;
								String selectConditions = rs.getString("select_conditions");
								if ((selectConditions != null) && (!selectConditions.isBlank())) {	
									conditions = getAllSelectConditions(rs.getString("object_name"), selectConditions);					
								}
								o = new DBObject(this.globalTracer, rs.getString("object_name"), rs.getString("allowed_columns"), rs.getString("unique_key_columns"), rs.getString("incremental_key_columns"), rs.getString("group_name"), rs.getInt("group_position"), rs.getString("mask_columns"), rs.getString("delete_condition"), 0, selectConditions, conditions);
								dbObjects.put(rs.getString("object_name"), o);
								addToObjectGroup(o);
							}
							
							if (o != null) {
								if ((ConfLoader.getInstance().getSrcDBReaderMethod() == DBReaderMethod.INCREMENTAL) && o.isIncrementalKeyConfigured()) {
									incrementalObjectCount++;
								} else {
									snapshotObjectCount++;
								}
							}
						}
					}
				}
			}
			
			//Update monitor
			Monitor.getInstance().setSnapshotObjectCount(snapshotObjectCount);
			Monitor.getInstance().setIncrementalObjectCount(incrementalObjectCount);
			Monitor.getInstance().setTotalObjects(dbObjects.size());
			Monitor.getInstance().setFailedObjects(0);
			
			//Reload object reload configurations
			try (Connection conn = DriverManager.getConnection(url)) {
				try(Statement stmt = conn.createStatement()) {
					try (ResultSet rs = stmt.executeQuery("SELECT object_name, reload_schema_on_next_restart, reload_schema_on_each_restart, reload_object_on_next_restart, reload_object_on_each_restart FROM src_object_reload_configurations")) {
						while(rs.next()) {
							String objName = rs.getString(1);
							Integer rsnr = rs.getInt(2);
							Integer rser = rs.getInt(3);
							Integer ronr = rs.getInt(4);
							Integer roer = rs.getInt(5);
							
							DBObject o = dbObjects.get(objName);
							if (o != null) {
								o.setObjectReloadConfigurations(rsnr, rser, ronr, roer);
							}
						}
					}
					//reset next restart configuration values 
					stmt.execute("UPDATE src_object_reload_configurations SET reload_schema_on_next_restart = 0, reload_object_on_next_restart = 0");
				}
			}
			this.globalTracer.info("Successfully loaded tables/views from metadata");
		} catch(Exception e) {
			throw new SyncLiteException("Failed to load tables/views from dbreader metadata file : " + this.dbReaderMetadataFile, e);
		}
	}

	private final HashSet<String> getObjectNamesFromMetadata() throws SyncLiteException {
		HashSet<String> objNames = new HashSet<String>();
		String url = "jdbc:sqlite:" + this.dbReaderMetadataFile;
		try (Connection conn = DriverManager.getConnection(url)) {
			try(Statement stmt = conn.createStatement()) {
				try (ResultSet rs = stmt.executeQuery("SELECT object_name from src_object_info")) {
					while (rs.next()) {
						objNames.add(rs.getString("object_name"));
					}
				}
			}
		} catch (Exception e) {
			throw new SyncLiteException("Failed to read object names from dbreader metadata file : " + this.dbReaderMetadataFile + " : " + e.getMessage(), e);
		}
		return objNames;
	}
	
	private final void addSrcObjectInfoToMetadataTable(ObjectInfo oInfo, String objectType) throws SyncLiteException {
		String url = "jdbc:sqlite:" + this.dbReaderMetadataFile;
		String metadataTableDeleteSql = "DELETE FROM src_object_info WHERE object_name = '" + oInfo.name + "'";
		String metadataTableInsertSql = "INSERT INTO src_object_info(object_name, object_type, allowed_columns, unique_key_columns, incremental_key_columns, group_name, group_position, mask_columns, delete_condition, select_conditions, enable) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		while(!Thread.interrupted()) {
			try (Connection conn = DriverManager.getConnection(url)){
				conn.setAutoCommit(false);		
				try (PreparedStatement pstmt = conn.prepareStatement(metadataTableInsertSql);
						Statement stmt = conn.createStatement()) {
					pstmt.setString(1, oInfo.name);
					pstmt.setString(2, objectType);
					pstmt.setString(3, oInfo.columnDefStr);
					pstmt.setString(4, oInfo.uniqueKeyColumnsStr);
					pstmt.setString(5, "");
					pstmt.setString(6, "");
					pstmt.setInt(7, 1);
					pstmt.setString(8, "");
					pstmt.setString(9, "");
					pstmt.setString(10, "");
					pstmt.setInt(11, 1);
					pstmt.addBatch();				
					stmt.execute(metadataTableDeleteSql);
					pstmt.executeBatch();
				}
				conn.commit();
				break;
			} catch (SQLException e) {
				//If we get SQLite Busy exception then retry.
				if (e.getMessage().contains("SQLITE_BUSY")) {
					//Retry
					continue;
				} else {
					this.globalTracer.error("Failed to insert object info in metadata file : " + dbReaderMetadataFile + " : " + e.getMessage(), e);
					throw new SyncLiteException("Failed to insert object info in metadata file : " + this.dbReaderMetadataFile + " : " + e.getMessage() , e);
				}
			}
		}
	}

	private void initSyncLiteDevices() throws SyncLiteException {
		try {
			this.globalTracer.info("Initializing SyncLite devices for DB objects");
			for (HashMap.Entry<String, DBObject> entry : dbObjects.entrySet()) {
				configureSyncLiteDevice(entry.getValue());
			}
			this.globalTracer.info("Finished initializing SyncLite devices DB objects");
		} catch (Exception e) {
			this.globalTracer.error("Failed while initializing SyncLite devices for DB objects : " + e.getMessage(), e);
			throw new SyncLiteException("Failed while initializing SyncLite devices for DB objects : " + e.getMessage(), e);
		}
	}
	
	private final void configureSyncLiteDevice(DBObject obj) throws SyncLiteException {
		obj.configureSyncLiteDevice();
	}
	
	private void addToObjectGroup(DBObject o) {
		//Add object to respective object group for scheduling
		DBObjectGroup grp = objectGroups.get(o.getDeviceName());
		if (grp == null) {
			grp = new DBObjectGroup();
			objectGroups.put(o.getDeviceName(), grp);
		}
		if (grp.objects == null) {
			grp.objects = new HashMap<Integer, DBObject>();
		} 
		grp.objects.put(o.getPosition(), o);
	}
	
	private final HashMap<Integer, String> getAllSelectConditions(String objectName, String selectConditions) throws SyncLiteException {
		try {
			org.json.JSONArray jsonArr = new org.json.JSONArray(new JSONTokener(selectConditions));
			HashMap<Integer, String> conditions = new HashMap<Integer, String>();
			int idx = 1;
			for (Object o : jsonArr) {
				conditions.put(idx, o.toString());
				++idx;
			}
			return conditions;
		} catch (Exception e) {
			throw new SyncLiteException("Invalid select conditions specified for object " + objectName, e);
		}
	}
	private final void initSourceDB() throws com.synclite.dbreader.SyncLiteException {
		switch(ConfLoader.getInstance().getSrcType()) {
		case CSV:
			Path dir = ConfLoader.getInstance().getSrcFileStorageLocalFSDirectory();
			if ((dir == null) || dir.toString().isBlank()) {
				this.globalTracer.error("Please verify a valid directory path.");
				throw new SyncLiteException("Please verify a valid directory path.");			
			}
			if (Files.exists(dir)) {
				if (!dir.toFile().isDirectory()) {
					this.globalTracer.error("The specified path is not a directory : " + dir + ".");
					throw new SyncLiteException("The specified path is not a directory : " + dir + ".");
				}
				if (!dir.toFile().canRead()) {
					this.globalTracer.error("The specified directory is not readable : " + dir + ".");
					throw new SyncLiteException("The specified directory is not readable : " + dir + ".");		
				}
			} else {
				this.globalTracer.error("The specified directory does not exist : " + dir + ".");
				throw new SyncLiteException("The specified directory does not exist : " + dir + ".");		
			}
			break;
		case MONGODB:
			break;
		default:
			try (Connection conn =  JDBCConnector.getInstance().connect()) {
				//Do nothing
			} catch (Exception e) {
				throw new SyncLiteException("Failed to connect to source database : ", e);
			}
		}
	}

	@Override
	public final void run() {
		try {
			initDriver();
			globalTracer.info("Initialized DB Reader job");
			if (Main.CMD == CMDType.READ) {
				if (ConfLoader.getInstance().getDBReaderStopAfterFirstIteration()) {
					globalTracer.info("Starting single iteration DB Reader job");
					runReadServicesOnce();
					globalTracer.info("Finished single iteration DB Reader job");
				} else {
					globalTracer.info("Starting DB Reader job");
					runReadServices();
					globalTracer.info("Finished DB Reader job");
				}
			} else if (Main.CMD == CMDType.DELETE_SYNC) {
				globalTracer.info("Starting Delete Sync job");
				runDeleteSyncServices();
				globalTracer.info("Finished Delete Sync job");
			}
		} catch (Exception e) {
			globalTracer.error("ERROR : ", e);
			System.out.println("ERROR : " + e.getMessage());
			System.exit(1);
		}
	}

	private final void scheduleObjects() {
		try {
			Monitor.getInstance().setLastReadStartTime(System.currentTimeMillis());
			for (DBObjectGroup grp : objectGroups.values()) {
				addObjectGroupTask(grp);
			}
		} catch(InterruptedException e) {
			Thread.interrupted();
		}
	}
	
	private final void detectNewObjects() {
		try {
			this.globalTracer.info("Attempting to detect newly created objects");
			//Read all object names from metadata file.			
			HashSet<String> existingObjNames = getObjectNamesFromMetadata();
			HashMap<String, String> allObjects = DBMetadataReader.loadObjectNamesFromMetadata();
			//Iterate and find new objects
			for (Map.Entry<String, String> entry : allObjects.entrySet()) {
				String objName = entry.getKey();
				String objType = entry.getValue();
			
				if (! existingObjNames.contains(objName)) {
					this.globalTracer.info("Object : " + objName + " detected.");
					//Add this object.
					ObjectInfo oInfo = DBMetadataReader.readSrcSchema(objName);
					
					//Persist newly detected object in metadata file.
					addSrcObjectInfoToMetadataTable(oInfo, objType);
					this.globalTracer.info("Object : " + objName + " schema info persisted to dbreader metadata.");

					//Create DBObject and enable for scheduling.
					DBObject obj = new DBObject(globalTracer, objName, oInfo.columnDefStr, oInfo.uniqueKeyColumnsStr, "", "", 1, "", "", 0, "", null);

					//Initialize device
					configureSyncLiteDevice(obj);
					this.globalTracer.info("SyncLite device initialized for object : " + objName);

					dbObjects.put(objName, obj);
					addToObjectGroup(obj);

					//Update monitor
					Monitor.getInstance().incrSnapshotObjectCount();
					Monitor.getInstance().setTotalObjects(dbObjects.size());

					this.globalTracer.info("Object : " + objName + " added to scheduler");
				}
			}
			this.globalTracer.info("Finished new object detection.");
		} catch (Exception e) {
			this.globalTracer.error("New Object detector failed, operation will be reattempted : " + e.getMessage(), e);
		}
	}

	private final void scheduleFailedObjectGroups() {
		try {
			Monitor.getInstance().setLastReadStartTime(System.currentTimeMillis());
			for (DBObjectGroup grp : failedDBObjectGroups.values()) {
				addObjectGroupTask(grp);
			}
		} catch(InterruptedException e) {
			Thread.interrupted();
		}
	}

	public final void removeObject(DBObject o) {
		dbObjects.remove(o);
		//Remove entry from TableGroup as well
		DBObjectGroup grp = objectGroups.get(o.getDeviceName());
		if (grp != null) {
			grp.objects.remove(o.getPosition());
			
			if (grp.objects.size() == 0) {
				//Remove object group as no object left 
				objectGroups.remove(o.getDeviceName());
				
				//Also remove this objectGroup from scheduled tasks
				while(tasks.remove(grp)) {
					;
				}
			}		
		}
	}
	
	public ConcurrentHashMap<String, DBObject> getDBObjects() {
		return this.dbObjects;
	}

	private void createSystemDeviceSendShutdown() throws SyncLiteException {
		try {
			Path deviceFilePath = ConfLoader.getInstance().getSyncLiteDeviceDir().resolve("synclite_dbreader_system.db");
			Class.forName("io.synclite.logger.Telemetry");
			Telemetry.initialize(deviceFilePath, ConfLoader.getInstance().getSyncLiteLoggerConfigurationFile(), "system");
			String deviceURL = "jdbc:synclite_telemetry:" + deviceFilePath;
			try (Connection conn = DriverManager.getConnection(deviceURL)) {
				try (TelemetryStatement stmt = (TelemetryStatement) conn.createStatement()) {
					stmt.log("SHUTDOWN");
				}
			}
			Telemetry.closeAllDevices();
		} catch (Exception e) {
			throw new SyncLiteException("Failed to create a system device and send SHUTDOWN message : " + e.getMessage(), e);
		}
	}
}
