package com.synclite.dbreader;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HikariPoolTest {
    @Test
    void readsSqliteThroughReusedPool(@TempDir Path directory) throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:sqlite:" + directory.resolve("source.db"));
        config.setMaximumPoolSize(1);
        config.setPoolName("dbreader-upgrade-test");
        try (HikariDataSource pool = new HikariDataSource(config)) {
            try (Connection connection = pool.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE sample (value INTEGER)");
                statement.execute("INSERT INTO sample VALUES (42)");
            }
            try (Connection connection = pool.getConnection();
                 Statement statement = connection.createStatement();
                 ResultSet rows = statement.executeQuery("SELECT value FROM sample")) {
                assertTrue(rows.next());
                assertEquals(42, rows.getInt(1));
                assertFalse(rows.next());
            }
        }
    }
}