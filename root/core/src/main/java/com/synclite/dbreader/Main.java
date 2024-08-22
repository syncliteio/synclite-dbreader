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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

public class Main {

	public static Path dbDir;
	private static Path dbReaderConfigFilePath;
	private static Path dbReaderArgFilePath;
	public static CMDType CMD;
	public static SyncLiteAppLock appLock = new SyncLiteAppLock();
	public static long jobStartTime = System.currentTimeMillis();
	public static void main(String[] args) {
		try {
			if ((args.length != 5) && (args.length != 7)) {
				usage();
			} else {
				String cmdTxt = args[0].trim().toLowerCase();
				
				if (cmdTxt.equals("read")) {
					CMD = CMDType.READ;
				} else if (cmdTxt.equals("delete-sync")) {
					CMD = CMDType.DELETE_SYNC;
				} else {
					usage();
				}

				if (!args[1].trim().equals("--db-dir")) {
					usage();
				} else {
					dbDir = Path.of(args[2]);
					if (!Files.exists(dbDir)) {
						error("Invalid work-dir specified");
					}
				}

				if (!args[3].trim().equals("--config")) {
					usage();
				} else {
					dbReaderConfigFilePath = Path.of(args[4]);
					if (!Files.exists(dbReaderConfigFilePath)) {
						error("Invalid dbreader configuration file specified : " + dbReaderConfigFilePath);
					}
				}			
				
				tryLockDBDir();

				ConfLoader.getInstance().loadDBReaderConfigProperties(dbReaderConfigFilePath);

				if (args.length == 7) {
					if (!args[5].trim().equals("--arguments")) {
						usage();
					} else {
						dbReaderArgFilePath = Path.of(args[6]);
						if (!Files.exists(dbReaderArgFilePath)) {
							error("Invalid dbreader arguments file specified : " + dbReaderArgFilePath);
						}
					}
					ConfLoader.getInstance().loadDBReaderArgProperties(dbReaderArgFilePath);
				}			
				
				DBReaderDriver.getInstance().run();
				System.exit(0);
			}
		} catch (Exception e) {		
			try {				 
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
				e.printStackTrace(pw);
				String stackTrace = sw.toString();
				Path exceptionFilePath; 
				if (dbDir == null) { 
					exceptionFilePath = Path.of("synclite_dbreader_exception.trace");
				} else {
					exceptionFilePath = dbDir.resolve("synclite_dbreader_exception.trace");
				}

				String finalStr = e.getMessage() + "\n" + stackTrace;
				Files.writeString(exceptionFilePath, finalStr);
				System.out.println("ERROR : " + finalStr);
				System.err.println("ERROR : " + finalStr);	
			} catch (Exception ex) {
				System.out.println("ERROR : " + ex);
				System.err.println("ERROR : " + ex);	
			}
			System.exit(1);
		}

	}

	private static final void tryLockDBDir() throws SyncLiteException {
		appLock.tryLock(dbDir);		
	}

	private static final void error(String message) throws Exception {
		System.out.println("ERROR : " + message);
		throw new Exception("ERROR : " + message);
	}

	private static final void usage() {
		System.out.println("Usage:"); 
		System.out.println("SyncLiteDBReader read --work-dir <path/to/work-dir> --config <path/to/dbreader_config>");
		System.out.println("SyncLiteDBReader read --work-dir <path/to/work-dir> --config <path/to/dbreader_config> --arguments <path/to/dbreader_args>");
		System.out.println("SyncLiteDBReader delete-sync --work-dir <path/to/work-dir> --config <path/to/dbreader_config>");
		System.exit(1);
	}
}
