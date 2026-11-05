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

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import io.synclite.S3ClientFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3FileStorage extends FileStorage {
	
    private static final class InstanceHolder {
        private static S3FileStorage INSTANCE = new S3FileStorage();
    }

    public static S3FileStorage getOrCreateInstance() {
    	return InstanceHolder.INSTANCE;
    }

	private Path localBaseDirectory;
	private S3Client s3Client;
	private String bucketName;
	protected S3FileStorage() {
		try {
			s3Client = S3ClientFactory.create(ConfLoader.getInstance().getSrcFileStorageS3Url(),
					ConfLoader.getInstance().getSrcFileStorageS3AccessKey(), ConfLoader.getInstance().getSrcFileStorageS3SecretKey());
			this.bucketName = ConfLoader.getInstance().getSrcFileStorageS3BucketName();
			//Validate if we are connected
			s3Client.headBucket(request -> request.bucket(this.bucketName));

			this.localBaseDirectory = ConfLoader.getInstance().getSrcFileStorageLocalFSDirectory();
		} catch (Exception e) {
			//tracer.error("Failed to create a client/connect to S3 : " + e.getMessage(), e);
			throw new RuntimeException("Failed to create a client/connect to S3 : " + e.getMessage(), e);
		}
	}

	@Override
	public void listFilesOrderByCreationTime(String objName, Instant thresholdTS, List<Path> fileList, HashMap<Path, Instant> creationTimes, Logger tracer) throws IOException {
		try {
			for (S3Object ob : s3Client.listObjectsV2Paginator(request -> request.bucket(bucketName).prefix(objName + "/")).contents()) {
				Path remoteFilePath = Path.of(ob.key());
				Instant createTime = ob.lastModified();
				if (thresholdTS != null) {
					if (createTime.isAfter(thresholdTS)) {
						fileList.add(remoteFilePath);
						creationTimes.put(remoteFilePath, createTime);
					}
				} else {
					fileList.add(remoteFilePath);
					creationTimes.put(remoteFilePath, createTime);
				}
			}
			
	        Collections.sort(fileList, Comparator.comparing(creationTimes::get));

		} catch (SdkException e) {
			String errorMsg = "Exception while listing files for object : " + objName + " : " + e.getMessage();
			tracer.error("Exception while listing files for object : " + objName + " : " + e.getMessage(), e);				
			throw new IOException(errorMsg, e);
		}
	}

	@Override
	public Path downloadFile(String objName, Path remotePath, Logger tracer) throws IOException {
		Path outputFilePath = this.localBaseDirectory.resolve(objName).resolve(remotePath.getFileName().toString());
		try {
			if (Files.exists(outputFilePath)) {
				Files.delete(outputFilePath);
			}

			Files.createFile(outputFilePath);
			try (ResponseInputStream<GetObjectResponse> inputStream = s3Client.getObject(request -> request.bucket(bucketName).key(remotePath.toString()))) {
				try (FileOutputStream outputStream = new FileOutputStream(outputFilePath.toFile())) {
					int read = 0;
					byte[] bytes = new byte[2048];
					while ((read = inputStream.read(bytes)) != -1) {
						outputStream.write(bytes, 0, read);
					}
					outputStream.flush();
				}
			}
			return outputFilePath;
		} catch(Exception e) {
			throw new IOException("Failed to download object : " + remotePath + " to local path : " + outputFilePath + " : " + e.getMessage(), e);
		}
	}

}
