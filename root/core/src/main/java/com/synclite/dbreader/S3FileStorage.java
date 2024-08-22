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

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.AwsHostNameUtils;

public class S3FileStorage extends FileStorage {
	
    private static final class InstanceHolder {
        private static S3FileStorage INSTANCE = new S3FileStorage();
    }

    public static S3FileStorage getOrCreateInstance() {
    	return InstanceHolder.INSTANCE;
    }

	private Path localBaseDirectory;
	private AmazonS3 s3Client;
	private String bucketName;
	protected S3FileStorage() {
		try {
	        AWSCredentials credentials = new BasicAWSCredentials(ConfLoader.getInstance().getSrcFileStorageS3AccessKey(), ConfLoader.getInstance().getSrcFileStorageS3SecretKey());

			s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withEndpointConfiguration(new EndpointConfiguration(ConfLoader.getInstance().getSrcFileStorageS3Url(), AwsHostNameUtils.parseRegion(ConfLoader.getInstance().getSrcFileStorageS3Url(), null)))
                    .build();
			this.bucketName = ConfLoader.getInstance().getSrcFileStorageS3BucketName();
			//Validate if we are connected
			s3Client.doesBucketExistV2(this.bucketName);

			this.localBaseDirectory = ConfLoader.getInstance().getSrcFileStorageLocalFSDirectory();
		} catch (Exception e) {
			//tracer.error("Failed to create a client/connect to S3 : " + e.getMessage(), e);
			throw new RuntimeException("Failed to create a client/connect to S3 : " + e.getMessage(), e);
		}
	}

	@Override
	public void listFilesOrderByCreationTime(String objName, Instant thresholdTS, List<Path> fileList, HashMap<Path, Instant> creationTimes, Logger tracer) throws IOException {
		try {
			ListObjectsV2Result result = s3Client.listObjectsV2(bucketName + "/" + objName);				
			for (S3ObjectSummary ob : result.getObjectSummaries()) {
				Path remoteFilePath = Path.of(ob.getKey());
				Instant createTime = ob.getLastModified().toInstant();
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

		} catch (AmazonClientException e) {
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
			S3Object s3Object = s3Client.getObject(bucketName, remotePath.toString());	            
			try (S3ObjectInputStream inputStream = s3Object.getObjectContent()) {
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
