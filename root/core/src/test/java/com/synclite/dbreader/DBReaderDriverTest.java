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
 */

package com.synclite.dbreader;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test for DBReaderDriver with a SQLite source database.
 *
 * <p>Creates a SQLite source DB with two tables – the primary table covers all
 * supported data type categories (TEXT, VARCHAR, INTEGER, SMALLINT, BIGINT,
 * REAL, DOUBLE, FLOAT, NUMERIC, DECIMAL, BOOLEAN, DATE, DATETIME, TIMESTAMP,
 * BLOB, CLOB) and a second helper table used for DROP TABLE detection.
 * Configures and runs DBReaderDriver in single-iteration mode with DDL change
 * capture enabled ({@code src-infer-schema-changes},
 * {@code src-infer-object-drop}).  Exercises three iterations:
 * <ol>
 *   <li>Initial read – creates devices and reads initial rows from both tables.</li>
 *   <li>Insert, update, soft-delete mutations – verifies incremental replay
 *       (updates appear as INSERTs, soft-deletes produce DELETEs).</li>
 *   <li>DDL mutations – ALTER TABLE ADD/DROP COLUMN on primary table and
 *       DROP TABLE on second table – verifies schema change and table drop
 *       detection in the command log.</li>
 * </ol>
 */
class DBReaderDriverTest {

    private static final String TEST_SUBDIR = "testdbreader";

    private Path syncliteRoot;
    private Path testHome;
    private Path dbDir;
    private Path deviceDir;
    private Path stageDir;
    private Path srcDbPath;
    private Path configPath;
    private Path loggerConfigPath;
    private Path metadataDbPath;
    private Thread driverThread;

    @BeforeEach
    void setUp() throws Exception {
        syncliteRoot = Path.of(System.getProperty("user.home"), "synclite");
        testHome = syncliteRoot.resolve("tests");
        dbDir = testHome.resolve("db").resolve("dbreader");
        // All test artifacts live under db/dbreader/testdbreader/
        deviceDir = dbDir.resolve(TEST_SUBDIR);
        stageDir = testHome.resolve("stageDir");
        // Source DB lives inside the test subfolder
        srcDbPath = deviceDir.resolve("srcDb").resolve("source.db");
        // Config files live inside the test subfolder
        configPath = deviceDir.resolve("synclite_dbreader.conf");
        loggerConfigPath = deviceDir.resolve("synclite.conf");
        metadataDbPath = deviceDir.resolve("synclite_dbreader_metadata.db");
        driverThread = null;

        // Clean up only this test's artifacts – leave other directories untouched
        if (Files.exists(deviceDir)) {
            deleteRecursively(deviceDir);
        }
        deleteDeviceStageDirs("testdbreader");
        deleteDeviceStageDirs("testdbreader2");
        deleteDeviceStageDirs("synclite_dbreader_system");

        Files.createDirectories(deviceDir);
        Files.createDirectories(stageDir);
        Files.createDirectories(srcDbPath.getParent());

        // --- Create synclite-logger configuration ---
        Files.writeString(loggerConfigPath,
                "local-data-stage-directory = " + stageDir + "\n" +
                "device-stage-type = FS\n");

        // --- Create source SQLite database with initial row ---
        // Table includes columns for all supported data type categories:
        //   String: TEXT, VARCHAR   Integer: INTEGER, SMALLINT, BIGINT
        //   Floating-point: REAL, DOUBLE, FLOAT   Fixed-point: NUMERIC, DECIMAL
        //   Boolean: BOOLEAN   Date/Time: DATE, DATETIME, TIMESTAMP
        //   Binary/LOB: BLOB, CLOB
        Class.forName("org.sqlite.JDBC");
        String srcUrl = "jdbc:sqlite:" + srcDbPath;
        try (Connection conn = DriverManager.getConnection(srcUrl);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE test_dbreader (" +
                    "id INTEGER PRIMARY KEY, " +
                    "col_text TEXT, " +
                    "col_varchar VARCHAR(100), " +
                    "col_int INTEGER, " +
                    "col_smallint SMALLINT, " +
                    "col_bigint BIGINT, " +
                    "col_real REAL, " +
                    "col_double DOUBLE, " +
                    "col_float FLOAT, " +
                    "col_numeric NUMERIC(10,2), " +
                    "col_decimal DECIMAL(8,4), " +
                    "col_boolean BOOLEAN, " +
                    "col_date DATE, " +
                    "col_datetime DATETIME, " +
                    "col_timestamp TIMESTAMP, " +
                    "col_blob BLOB, " +
                    "col_clob CLOB, " +
                    "is_deleted INTEGER DEFAULT 0, " +
                    "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
            stmt.execute("INSERT INTO test_dbreader " +
                    "(id, col_text, col_varchar, col_int, col_smallint, col_bigint, " +
                    " col_real, col_double, col_float, col_numeric, col_decimal, " +
                    " col_boolean, col_date, col_datetime, col_timestamp, " +
                    " col_blob, col_clob, is_deleted, updated_at) VALUES " +
                    "(1, 'hello world', 'varchar value', 42, 7, 9999999999, " +
                    " 3.14, 2.718281828, 1.5, 12345.67, 99.1234, " +
                    " 1, '2025-01-15', '2025-06-15 10:30:00', '2025-01-01 12:00:00', " +
                    " X'DEADBEEF', 'clob text value', 0, '2025-01-01 00:00:01')");

            // Second table used to test DROP TABLE detection
            stmt.execute("CREATE TABLE test_dbreader2 (" +
                    "id INTEGER PRIMARY KEY, " +
                    "name TEXT, " +
                    "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
            stmt.execute("INSERT INTO test_dbreader2 (id, name, updated_at) VALUES " +
                    "(1, 'row one', '2025-01-01 00:00:01')");
        }

        // --- Create dbreader config file ---
        Files.writeString(configPath,
                "synclite-device-dir = " + deviceDir + "\n" +
                "synclite-logger-configuration-file = " + loggerConfigPath + "\n" +
                "src-type = SQLITE\n" +
                "src-connection-string = jdbc:sqlite:" + srcDbPath + "\n" +
                "src-connection-timeout-s = 30\n" +
                "src-dbreader-interval-s = 2\n" +
                "src-dbreader-batch-size = 100000\n" +
                "src-dbreader-processors = 1\n" +
                "src-dbreader-method = INCREMENTAL\n" +
                "dbreader-stop-after-first-iteration = false\n" +
                "src-object-type = TABLE\n" +
                "src-default-unique-key-column-list = id\n" +
                "src-default-incremental-key-column-list = updated_at\n" +
                "src-timestamp-incremental-key-initial-value = 0001-01-01 00:00:00\n" +
                "src-default-soft-delete-condition = is_deleted = 1\n" +
                "src-infer-schema-changes = true\n" +
                "src-infer-object-drop = true\n" +
                "dbreader-trace-level = DEBUG\n" +
                "dbreader-update-statistics-interval-s = 5\n" +
                "dbreader-enable-statistics-collector = true\n" +
                "edition = DEVELOPER\n");

        // --- Create metadata database (normally created by the web UI) ---
        String metaUrl = "jdbc:sqlite:" + metadataDbPath;
        try (Connection conn = DriverManager.getConnection(metaUrl);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS src_object_info(" +
                    "object_name TEXT PRIMARY KEY, " +
                    "object_type TEXT, " +
                    "allowed_columns TEXT, " +
                    "unique_key_columns TEXT, " +
                    "incremental_key_columns TEXT, " +
                    "group_name TEXT, " +
                    "group_position INTEGER, " +
                    "mask_columns TEXT, " +
                    "delete_condition TEXT, " +
                    "select_conditions TEXT, " +
                    "enable INTEGER)");

            stmt.execute("CREATE TABLE IF NOT EXISTS src_object_reload_configurations(" +
                    "object_name TEXT PRIMARY KEY, " +
                    "reload_schema_on_next_restart INT, " +
                    "reload_schema_on_each_restart INT, " +
                    "reload_object_on_next_restart INT, " +
                    "reload_object_on_each_restart INT)");

            // Register test_dbreader table: all columns allowed (as JSON array), id as unique key,
            // updated_at as incremental key, is_deleted = 1 as soft delete condition.
            // Read column definitions from JDBC metadata so the format exactly matches
            // what readSrcSchema() produces (prevents false ALTER COLUMN detections).
            String allowedColumns;
            try (Connection srcConn = DriverManager.getConnection("jdbc:sqlite:" + srcDbPath)) {
                allowedColumns = readJdbcSchemaAsJson(srcConn, "test_dbreader");
            }
            stmt.execute("INSERT INTO src_object_info VALUES(" +
                    "'test_dbreader', 'TABLE', " +
                    "'" + allowedColumns.replace("'", "''") + "', " +
                    "'id', 'updated_at', " +
                    "'', 1, '', 'is_deleted = 1', '', 1)");

            stmt.execute("INSERT INTO src_object_reload_configurations VALUES(" +
                    "'test_dbreader', 0, 0, 0, 0)");

            // Register second table (for DROP TABLE detection test)
            String allowedColumns2;
            try (Connection srcConn = DriverManager.getConnection("jdbc:sqlite:" + srcDbPath)) {
                allowedColumns2 = readJdbcSchemaAsJson(srcConn, "test_dbreader2");
            }
            stmt.execute("INSERT INTO src_object_info VALUES(" +
                    "'test_dbreader2', 'TABLE', " +
                    "'" + allowedColumns2.replace("'", "''") + "', " +
                    "'id', 'updated_at', " +
                    "'', 1, '', '', '', 1)");

            stmt.execute("INSERT INTO src_object_reload_configurations VALUES(" +
                    "'test_dbreader2', 0, 0, 0, 0)");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            DBReaderDriver.getInstance().stopSyncServices();
        } catch (Exception ignored) {
        }
        // Wait for the background driver thread to finish
        if (driverThread != null) {
            driverThread.join(10_000);
        }
        try {
            io.synclite.logger.DBLogger.closeAllDevices();
        } catch (Exception ignored) {
        }
        // Release the app lock so the lock file can be deleted on next setUp
        try {
            java.lang.reflect.Field lockField = SyncLiteAppLock.class.getDeclaredField("lock");
            lockField.setAccessible(true);
            Connection lockConn = (Connection) lockField.get(Main.appLock);
            if (lockConn != null) lockConn.close();
        } catch (Exception ignored) {
        }
        // Give Windows time to release file locks
        Thread.sleep(200);

        System.out.println("\n[TEST ARTIFACTS] Location: " + deviceDir + "\n");
    }

    @Test
    void testIncrementalReadWithInsertUpdateSoftDelete() throws Exception {
        // --- Start the reader in continuous scheduler mode on a background thread ---
        startDBReader();

        // --- Wait for iteration 1: initial row is read ---
        // The scheduler picks up both tables and reads the initial data.
        // We wait until the checkpoint for test_dbreader advances past "0".
        waitForCheckpoint("test_dbreader", "updated_at", "2025-01-01 00:00:01");

        // --- Mutate source DB: insert, update, soft-delete ---
        String srcUrl = "jdbc:sqlite:" + srcDbPath;
        try (Connection conn = DriverManager.getConnection(srcUrl);
             Statement stmt = conn.createStatement()) {
            // Insert a new row with different values for every data-type column
            stmt.execute("INSERT INTO test_dbreader " +
                    "(id, col_text, col_varchar, col_int, col_smallint, col_bigint, " +
                    " col_real, col_double, col_float, col_numeric, col_decimal, " +
                    " col_boolean, col_date, col_datetime, col_timestamp, " +
                    " col_blob, col_clob, is_deleted, updated_at) VALUES " +
                    "(2, 'second row', 'another varchar', 84, 14, 1234567890123, " +
                    " 6.28, 1.41421356, 2.5, 67890.12, 55.6789, " +
                    " 0, '2025-02-20', '2025-07-20 14:45:00', '2025-02-01 08:30:00', " +
                    " X'CAFEBABE', 'second clob value', 0, '2025-01-01 00:00:02')");
            // Update an existing row – change values across multiple type columns
            stmt.execute("UPDATE test_dbreader SET " +
                    "col_text = 'updated text', col_int = 999, col_real = 9.99, " +
                    "col_boolean = 0, col_blob = X'0102030405', " +
                    "updated_at = '2025-01-01 00:00:03' WHERE id = 1");
            // Soft-delete: set is_deleted = 1 (bumps updated_at)
            stmt.execute("UPDATE test_dbreader SET is_deleted = 1, updated_at = '2025-01-01 00:00:04' " +
                    "WHERE id = 2");
        }

        // --- Wait for iteration 2: mutations are read ---
        waitForCheckpoint("test_dbreader", "updated_at", "2025-01-01 00:00:04");

        // --- Mutate source DB: ALTER TABLE schema changes + DROP second table ---
        try (Connection conn = DriverManager.getConnection(srcUrl);
             Statement stmt = conn.createStatement()) {
            // Add a new column to test_dbreader
            stmt.execute("ALTER TABLE test_dbreader ADD COLUMN col_new TEXT");
            // Drop an existing column
            stmt.execute("ALTER TABLE test_dbreader DROP COLUMN col_clob");
            // Bump updated_at so the altered row is picked up incrementally
            stmt.execute("UPDATE test_dbreader SET col_new = 'new value', " +
                    "updated_at = '2025-01-01 00:00:05' WHERE id = 1");
            // Drop second table entirely
            stmt.execute("DROP TABLE test_dbreader2");
        }

        // --- Wait for iteration 3: schema changes and drop detected ---
        waitForCheckpoint("test_dbreader", "updated_at", "2025-01-01 00:00:05");

        // Stop the scheduler and flush device logs to the stage directory
        DBReaderDriver.getInstance().stopSyncServices();
        if (driverThread != null) {
            driverThread.join(10_000);
        }
        io.synclite.logger.DBLogger.closeAllDevices();

        // --- Validate: device archive exists in stageDir ---
        assertTrue(Files.exists(stageDir), "Stage directory should exist");

        // Find stage subdirectory matching synclite-<deviceName>-<uuid>
        // The device name for table "test_dbreader" is "testdbreader" (non-alphanumeric stripped)
        Path deviceStageSubDir = findDeviceStageDir("testdbreader");
        assertNotNull(deviceStageSubDir, "Device stage directory for 'testdbreader' should exist under stageDir");

        // Find stage subdirectory for second table
        Path deviceStageSubDir2 = findDeviceStageDir("testdbreader2");
        assertNotNull(deviceStageSubDir2, "Device stage directory for 'testdbreader2' should exist under stageDir");

        // --- Validate: collect all commandlog entries from all .sqllog files ---
        List<String> allSqlStatements = collectCommandLogSql(deviceStageSubDir);

        // The DBLogger batches rows: each INSERT in the commandlog is a batch template
        // (VALUES(?,?,?...)) covering all rows read in that iteration.
        // With 3 iterations we expect:
        //   - 1 CREATE TABLE
        //   - 3 INSERT batches (1 per iteration)
        //   - 2+ DELETE statements (soft-delete condition applied per batch)
        //   - ALTER TABLE ADD COLUMN (col_new added in iteration 3)
        //   - ALTER TABLE DROP COLUMN (col_clob dropped in iteration 3)
        //   - REFRESH TABLE (after schema change detection)
        //   - 0 UPDATE statements (incremental mode replays all changes as INSERTs)

        long createCount = allSqlStatements.stream()
                .filter(sql -> sql.toUpperCase().contains("CREATE TABLE"))
                .count();
        long insertBatchCount = allSqlStatements.stream()
                .filter(sql -> sql.toUpperCase().startsWith("INSERT"))
                .count();
        long deleteCount = allSqlStatements.stream()
                .filter(sql -> sql.toUpperCase().startsWith("DELETE"))
                .count();
        long updateCount = allSqlStatements.stream()
                .filter(sql -> sql.toUpperCase().startsWith("UPDATE"))
                .count();
        long addColumnCount = allSqlStatements.stream()
                .filter(sql -> sql.toUpperCase().contains("ADD COLUMN"))
                .count();
        long dropColumnCount = allSqlStatements.stream()
                .filter(sql -> sql.toUpperCase().contains("DROP COLUMN"))
                .count();
        long refreshTableCount = allSqlStatements.stream()
                .filter(sql -> sql.toUpperCase().contains("REFRESH TABLE"))
                .count();

        System.out.println("=== Command Log SQL Statements (test_dbreader) ===");
        for (int i = 0; i < allSqlStatements.size(); i++) {
            System.out.println("[" + i + "] " + allSqlStatements.get(i));
        }
        System.out.println("CREATE count: " + createCount +
                ", INSERT batch count: " + insertBatchCount +
                ", DELETE count: " + deleteCount +
                ", UPDATE count: " + updateCount +
                ", ADD COLUMN count: " + addColumnCount +
                ", DROP COLUMN count: " + dropColumnCount +
                ", REFRESH TABLE count: " + refreshTableCount);

        assertTrue(createCount >= 1,
                "Should have at least 1 CREATE TABLE in command log, found: " + createCount);
        // 3 INSERT batches: one per iteration (iteration 3 has the schema-changed row)
        assertTrue(insertBatchCount >= 3,
                "Should have at least 3 INSERT batches (1 per iteration), found: " + insertBatchCount);
        // Soft-delete condition fires a DELETE per batch
        assertTrue(deleteCount >= 2,
                "Should have at least 2 DELETE statements (soft-delete per batch), found: " + deleteCount);
        // Updates are replayed as INSERTs – no UPDATE statements should appear
        assertEquals(0, updateCount,
                "UPDATEs should be replayed as INSERTs (no UPDATE in command log)");
        // ALTER TABLE ADD COLUMN for col_new
        assertTrue(addColumnCount >= 1,
                "Should have at least 1 ALTER TABLE ADD COLUMN, found: " + addColumnCount);
        // ALTER TABLE DROP COLUMN for col_clob
        assertTrue(dropColumnCount >= 1,
                "Should have at least 1 ALTER TABLE DROP COLUMN, found: " + dropColumnCount);
        // REFRESH TABLE after schema change detection
        assertTrue(refreshTableCount >= 1,
                "Should have at least 1 REFRESH TABLE after schema change, found: " + refreshTableCount);

        // --- Validate DROP TABLE for second table ---
        List<String> allSqlStatements2 = collectCommandLogSql(deviceStageSubDir2);

        System.out.println("=== Command Log SQL Statements (test_dbreader2) ===");
        for (int i = 0; i < allSqlStatements2.size(); i++) {
            System.out.println("[" + i + "] " + allSqlStatements2.get(i));
        }

        long dropTableCount = allSqlStatements2.stream()
                .filter(sql -> sql.toUpperCase().contains("DROP TABLE"))
                .count();
        System.out.println("DROP TABLE count: " + dropTableCount);

        assertTrue(dropTableCount >= 1,
                "Should have at least 1 DROP TABLE IF EXISTS for test_dbreader2, found: " + dropTableCount);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Starts DBReaderDriver in continuous scheduler mode on a daemon thread.
     * Uses the public {@code run()} API – no reflection needed for the read
     * path.  The scheduler reads objects at the configured interval (2 s).
     * Call {@link #waitForCheckpoint} to block until a specific round of
     * changes has been picked up.
     */
    private void startDBReader() throws Exception {
        Main.dbDir = deviceDir;
        Main.CMD = CMDType.READ;
        Main.appLock.tryLock(deviceDir);
        ConfLoader.getInstance().loadDBReaderConfigProperties(configPath);

        driverThread = new Thread(DBReaderDriver.getInstance()::run, "dbreader-test");
        driverThread.setDaemon(true);
        driverThread.start();
    }

    /**
     * Polls until the named object's incremental key checkpoint reaches (or
     * exceeds) the expected value, meaning the scheduler has finished reading
     * all rows up to that point.  Times out after 30 seconds.
     */
    private void waitForCheckpoint(String objectName, String keyColumn, String expectedVal)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            ConcurrentHashMap<String, DBObject> objects =
                    DBReaderDriver.getInstance().getDBObjects();
            DBObject obj = objects.get(objectName);
            if (obj != null) {
                String val = obj.getLastReadIncrementalKeyColVal(keyColumn);
                if (val != null && val.compareTo(expectedVal) >= 0) {
                    return;
                }
            }
            Thread.sleep(500);
        }
        fail("Timed out waiting for checkpoint " + objectName + "." +
             keyColumn + " >= " + expectedVal);
    }

    /**
     * Finds the device-specific stage subdirectory under stageDir.
     * Pattern: synclite-{deviceName}-{uuid}
     */
    private Path findDeviceStageDir(String deviceNamePrefix) throws IOException {
        if (!Files.exists(stageDir)) return null;
        try (var dirs = Files.list(stageDir)) {
            return dirs.filter(p -> Files.isDirectory(p) &&
                            p.getFileName().toString().startsWith("synclite-" + deviceNamePrefix + "-"))
                    .findFirst()
                    .orElse(null);
        }
    }

    /**
     * Collects all SQL statements from commandlog tables in all .sqllog files
     * within the given device stage directory.
     */
    private List<String> collectCommandLogSql(Path deviceStageDir) throws Exception {
        Pattern sqllogPattern = Pattern.compile("^\\d+\\.sqllog$");
        List<String> allSql = new ArrayList<>();

        try (var files = Files.walk(deviceStageDir)) {
            List<Path> logFiles = files
                    .filter(Files::isRegularFile)
                    .filter(p -> sqllogPattern.matcher(p.getFileName().toString()).matches())
                    .sorted()
                    .collect(Collectors.toList());

            for (Path logFile : logFiles) {
                String logUrl = "jdbc:sqlite:" + logFile;
                try (Connection conn = DriverManager.getConnection(logUrl);
                     Statement stmt = conn.createStatement()) {
                    // Check if commandlog table exists
                    try (ResultSet tables = conn.getMetaData().getTables(null, null, "commandlog", null)) {
                        if (!tables.next()) continue;
                    }
                    try (ResultSet rs = stmt.executeQuery(
                            "SELECT sql FROM commandlog ORDER BY change_number ASC")) {
                        while (rs.next()) {
                            String sql = rs.getString("sql");
                            if (sql != null && !sql.isBlank()) {
                                allSql.add(sql);
                            }
                        }
                    }
                }
            }
        }
        return allSql;
    }

    /**
     * Deletes device stage directories from stageDir that match the given device name prefix.
     * Pattern: synclite-{prefix}-{uuid}
     */
    private void deleteDeviceStageDirs(String prefix) throws IOException {
        if (!Files.exists(stageDir)) return;
        try (var dirs = Files.list(stageDir)) {
            List<Path> toDelete = dirs
                    .filter(p -> Files.isDirectory(p) &&
                                 p.getFileName().toString().startsWith("synclite-" + prefix + "-"))
                    .collect(Collectors.toList());
            for (Path dir : toDelete) {
                deleteRecursively(dir);
            }
        }
    }

    /**
     * Reads column definitions from JDBC metadata in the same format that
     * {@code DBMetadataReader.readSrcSchema()} produces, so the schema
     * comparison in {@code processObject()} sees no false differences.
     */
    private String readJdbcSchemaAsJson(Connection conn, String tableName) throws Exception {
        DatabaseMetaData meta = conn.getMetaData();
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        try (ResultSet cols = meta.getColumns(null, null, tableName, null)) {
            while (cols.next()) {
                String colName = cols.getString("COLUMN_NAME");
                String typeName = cols.getString("TYPE_NAME").toUpperCase();
                int colSize = cols.getInt("COLUMN_SIZE");
                int decDigits = cols.getInt("DECIMAL_DIGITS");
                String isNullable = cols.getString("IS_NULLABLE");

                StringBuilder typeBuilder = new StringBuilder(typeName);
                if (typeName.equals("DECIMAL") && colSize > 0) {
                    typeBuilder.append("(").append(colSize);
                    if (decDigits > 0) typeBuilder.append(", ").append(decDigits);
                    typeBuilder.append(")");
                } else if ((typeName.equals("VARCHAR") || typeName.equals("CHAR") ||
                            typeName.equals("NCHAR") || typeName.equals("NVARCHAR")) && colSize > 0) {
                    typeBuilder.append("(").append(colSize).append(")");
                }
                String nullable = (isNullable.equalsIgnoreCase("NO") ||
                                   isNullable.equalsIgnoreCase("N") ||
                                   isNullable.equals("0") ||
                                   isNullable.equalsIgnoreCase("FALSE") ||
                                   isNullable.equalsIgnoreCase("NOT NULL"))
                        ? "NOT NULL" : "NULL";
                typeBuilder.append(" ").append(nullable);

                if (!first) sb.append(", ");
                first = false;
                sb.append("\"").append(colName).append(" ").append(typeBuilder).append("\"");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    private void deleteRecursively(Path path) throws IOException {
        if (Files.notExists(path)) return;
        if (Files.isDirectory(path)) {
            try (var stream = Files.list(path)) {
                for (Path child : stream.collect(Collectors.toList())) {
                    deleteRecursively(child);
                }
            }
        }
        // On Windows, files may still be locked briefly after JVM closes handles.
        // Retry a few times before giving up.
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                Files.deleteIfExists(path);
                return;
            } catch (java.nio.file.FileSystemException e) {
                if (attempt == 4) throw e;
                try { Thread.sleep(200); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
            }
        }
    }
}
