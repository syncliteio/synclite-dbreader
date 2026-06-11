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

public final class MetadataManager {
	/** Bump when on-disk layout/semantics of {@code synclite_dbreader_metadata.db} change
	 *  in a non-back-compatible way. Stored in the {@code metadata} table under
	 *  {@link #SYNCLITE_METADATA_VERSION_KEY} so a future dbreader version can detect an
	 *  older store and run a migration routine. */
	public static final long SYNCLITE_METADATA_VERSION = 1L;
	public static final String SYNCLITE_METADATA_VERSION_KEY = "synclite_metadata_version";

	private MetadataManager() {}

	/** Ensure the {@code metadata} key/value table exists in the dbreader metadata file
	 *  and the {@link #SYNCLITE_METADATA_VERSION_KEY} row is seeded if absent. Idempotent. */
	public static void ensureMetadataTable(Path metadataFilePath) throws SQLException {
		String url = "jdbc:sqlite:" + metadataFilePath;
		try (Connection conn = DriverManager.getConnection(url);
				Statement stmt = conn.createStatement()) {
			stmt.execute("CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT)");
			seedMetadataVersionIfAbsent(conn);
		}
	}

	private static void seedMetadataVersionIfAbsent(Connection conn) throws SQLException {
		try (PreparedStatement sel = conn.prepareStatement(
				"SELECT 1 FROM metadata WHERE key = ?")) {
			sel.setString(1, SYNCLITE_METADATA_VERSION_KEY);
			try (ResultSet rs = sel.executeQuery()) {
				if (rs.next()) {
					return;
				}
			}
		}
		try (PreparedStatement ins = conn.prepareStatement(
				"INSERT INTO metadata(key, value) VALUES(?, ?)")) {
			ins.setString(1, SYNCLITE_METADATA_VERSION_KEY);
			ins.setString(2, Long.toString(SYNCLITE_METADATA_VERSION));
			ins.executeUpdate();
		}
	}
}
