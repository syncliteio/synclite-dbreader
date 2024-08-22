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
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;

public class SFTPFileStorage extends FileStorage {

    private static final class InstanceHolder {
        private static SFTPFileStorage INSTANCE = new SFTPFileStorage();
    }

    public static SFTPFileStorage getOrCreateInstance() {
    	return InstanceHolder.INSTANCE;
    }

    private Path localBaseDirectory;

	private String host;
	private Integer port;
	private String user;
	private String password;
	private String remoteDirectory;
	private Session masterSession;
	private ChannelSftp masterChannel;
	private ConcurrentHashMap<String, Session> containerSessions;    
	private ConcurrentHashMap<String, ChannelSftp> containerChannels;

	protected SFTPFileStorage() {
		super();
		try {
			this.host = ConfLoader.getInstance().getSrcFileStorageSFTPHost();
			this.port = ConfLoader.getInstance().getSrcFileStorageSFTPPort();
			this.user = ConfLoader.getInstance().getSrcFileStorageSFTPUser();
			this.password = ConfLoader.getInstance().getSrcFileStorageSFTPPassword();
			this.remoteDirectory = ConfLoader.getInstance().getSrcFileStorageSFTPDirectory();
			connect();

			//Check if the specified remote data stage dir exists.
			masterChannel.lstat(remoteDirectory.toString());

			containerSessions = new ConcurrentHashMap<String, Session>();
			containerChannels = new ConcurrentHashMap<String, ChannelSftp>();

			this.localBaseDirectory = ConfLoader.getInstance().getSrcFileStorageLocalFSDirectory();

		} catch (Exception e) {
			//tracer.error("Failed to connect to stage SFTP server : " + e.getMessage(), e);
			throw new RuntimeException("Failed to connect to specified SFTP server : " + e.getMessage(), e);
		}	

	}

	private final void connect() throws SyncLiteException {
		try {
			java.util.Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");
			JSch jsch = new JSch();
			//jsch.addIdentity("/home/ubuntu/.ssh/id_rsa");
			masterSession=jsch.getSession(user, host, port);
			//session.setConfig("PreferredAuthentications", "publickey, keyboard-interactive,password");
			masterSession.setPassword(password);            
			masterSession.setConfig(config);
			masterSession.setTimeout(Integer.MAX_VALUE);
			masterSession.connect();
			masterChannel = (ChannelSftp) masterSession.openChannel("sftp");
			masterChannel.connect(Integer.MAX_VALUE);
		} catch (Exception e) {
			throw new SyncLiteException("SFTP Connection failed with exception : " + e.getMessage(), e);
		}
	}

	private final void connectContainer(String object) throws SyncLiteException {
		try {
			java.util.Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");
			JSch jsch = new JSch();
			//jsch.addIdentity("/home/ubuntu/.ssh/id_rsa");
			Session s=jsch.getSession(user, host, port);
			//session.setConfig("PreferredAuthentications", "publickey, keyboard-interactive,password");
			s.setPassword(password);            
			s.setConfig(config);
			s.setTimeout(Integer.MAX_VALUE);
			s.connect();
			ChannelSftp c = (ChannelSftp) s.openChannel("sftp");
			c.connect(Integer.MAX_VALUE);
			containerSessions.put(object, s);
			containerChannels.put(object, c);            
		} catch (Exception e) {
			throw new SyncLiteException("SFTP Connection failed with exception while connecting for object : " + object + " : " + e.getMessage(), e);
		}
	}

	private final boolean isContainerConnected(String containerName) {
		Session s = containerSessions.get(containerName);
		ChannelSftp c = containerChannels.get(containerName);

		if (s == null) {
			return false;
		}
		if (c == null) {
			return false;
		}
		if (!s.isConnected()) {
			return false;
		}
		if (!c.isConnected()) {
			return false;
		}
		if(c.isClosed()) {
			return false;
		}
		return true;
	}


	@Override
	public void listFilesOrderByCreationTime(String objName, Instant thresholdTS, List<Path> fileList,HashMap<Path, Instant> creationTimes, Logger tracer) throws IOException {
		try {
			if (!isContainerConnected(objName)) {
				connectContainer(objName);
			}

			String objPath = remoteDirectory + "/" + objName;
			ChannelSftp c = containerChannels.get(objName);

			c.cd(objPath);

			Vector<ChannelSftp.LsEntry> entries = c.ls(objPath);

			for (ChannelSftp.LsEntry entry : entries) {
				if (!entry.getAttrs().isDir() && !entry.getFilename().equals(".") && !entry.getFilename().equals("..")) {
					Path remoteObjectPath = Path.of(objPath, entry.getFilename()); 

					SftpATTRS attributes = entry.getAttrs();
					long publishTime = attributes.getMTime(); 
					Date dt = new Date(publishTime);
					Instant createTime = dt.toInstant();
					
					if (thresholdTS != null) {
						if (createTime.isAfter(thresholdTS)) {
							fileList.add(remoteObjectPath);					
							creationTimes.put(remoteObjectPath, createTime);
						}
					} else {
						fileList.add(remoteObjectPath);					
						creationTimes.put(remoteObjectPath, createTime);
					}
				}			
		        Collections.sort(fileList, Comparator.comparing(creationTimes::get));

			}
		} catch (Exception e) {
			throw new IOException("Failed to list files for object : " + objName + " : " + e.getMessage(), e);
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

			ChannelSftp c = containerChannels.get(objName);
			// Download the file
			try (OutputStream outputStream = new FileOutputStream(outputFilePath.toString())) {
				c.get(remotePath.toString().replace("\\", "/"), outputStream);
			}
			
			return outputFilePath;
		} catch(Exception e) {
			throw new IOException("Failed to download object : " + remotePath + " to local path : " + outputFilePath + " : " + e.getMessage(), e);
		}
	}

}
