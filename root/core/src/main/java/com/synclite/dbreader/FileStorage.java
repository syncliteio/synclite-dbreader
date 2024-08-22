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

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

public abstract class FileStorage {
	
	protected FileStorage() {
	}
	
	public static FileStorage getInstance(Logger tracer) throws SyncLiteException {
		switch(ConfLoader.getInstance().getSrcFileStorageType()) {
		case LOCAL_FS:
			FileStorage s = LocalFileStorage.getOrCreateInstance();
			return s;
		case S3:
			s = S3FileStorage.getOrCreateInstance();
			return s;
		case SFTP:
			s = SFTPFileStorage.getOrCreateInstance();
			return s;
		}
		throw new SyncLiteException("Unsupported File storage type : " + ConfLoader.getInstance().getSrcFileStorageType());
	}
	
	public abstract void listFilesOrderByCreationTime(String objName, Instant thresholdTS, List<Path> fileList, HashMap<Path, Instant> creationTimes, Logger tracer) throws IOException;
	public abstract Path downloadFile(String objName, Path remotePath, Logger tracer) throws IOException;
}
