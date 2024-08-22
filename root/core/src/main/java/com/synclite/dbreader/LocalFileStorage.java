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
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

public class LocalFileStorage extends FileStorage {

    private static final class InstanceHolder {
        private static LocalFileStorage INSTANCE = new LocalFileStorage();
    }

    public static LocalFileStorage getOrCreateInstance() {
    	return InstanceHolder.INSTANCE;
    }
    
	private Path localBaseDirectory;
	protected LocalFileStorage() {
		super();
		this.localBaseDirectory = ConfLoader.getInstance().getSrcFileStorageLocalFSDirectory();
	}

	@Override
	public void listFilesOrderByCreationTime(String objName, Instant thresholdTS, List<Path> fileList,HashMap<Path, Instant> creationTimes, Logger tracer) throws IOException {
		Path directory = this.localBaseDirectory.resolve(objName);
		if (Files.exists(directory) && Files.isDirectory(directory)) {
			Files.walkFileTree(directory, new FileCreationTimeVisitor(fileList, thresholdTS, creationTimes));
			fileList.sort(Comparator.comparing(path -> {
				try {
					return Files.readAttributes(path, BasicFileAttributes.class).creationTime();
				} catch (IOException e) {
					tracer.error("Failed to read file attributes for file : " + path + " : " + e.getMessage(), e);
					return null;
				}
			}));
		}
	}

	private class FileCreationTimeVisitor implements FileVisitor<Path> {
		private final List<Path> fileList;
		private final HashMap<Path, Instant> creationTimes;
		private final Instant thresholdTime;

		public FileCreationTimeVisitor(List<Path> fileList, Instant thresholdTime, HashMap<Path, Instant> creationTimes) {
			this.fileList = fileList;
			this.creationTimes = creationTimes;
			this.thresholdTime = thresholdTime;
		}

		@Override
		public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			if (thresholdTime != null) {
				Instant creationTime = Files.readAttributes(file, BasicFileAttributes.class).creationTime().toInstant();
				if (creationTime.isAfter(thresholdTime)) {
					fileList.add(file);
					creationTimes.put(file, creationTime);
				}
			} else {
				fileList.add(file);
				creationTimes.put(file, null);
			}
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFileFailed(Path file, IOException exc) {
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
			return FileVisitResult.CONTINUE;
		}
	}

	@Override
	public Path downloadFile(String objName, Path remotePath, Logger tracer) throws IOException {
		return remotePath;
	}

}
