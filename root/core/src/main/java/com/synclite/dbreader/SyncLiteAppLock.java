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
import java.sql.Statement;

public class SyncLiteAppLock {
	
	private Connection lock;
	public SyncLiteAppLock() {
	}
	
	public final void tryLock(Path workDir) throws SyncLiteException {		
		Path lockFile = workDir.resolve("synclite_dbreader.lock");
		String lockFileURL = "jdbc:sqlite:" + lockFile;
		try {
			this.lock = DriverManager.getConnection(lockFileURL);
			try (Statement stmt = this.lock.createStatement()) {
	            stmt.executeUpdate("PRAGMA locking_mode = EXCLUSIVE");
	            stmt.executeUpdate("BEGIN EXCLUSIVE");
			}
		} catch (Exception e) {
			throw new SyncLiteException("Failed to lock synclite dbreader lock " + lockFile + ". Please stop any other dbreader job running for the specified db-dir : " + workDir);
		}
	}

}
