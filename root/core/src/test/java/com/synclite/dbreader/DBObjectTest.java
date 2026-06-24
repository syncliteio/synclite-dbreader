package com.synclite.dbreader;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

class DBObjectTest {

    @Test
    void numericIncrementalKeySqlUsesPlaceholderForComparison() throws Exception {
        configureDefaults();

        String allowedColumns = "[\"id BIGINT\",\"updated_at BIGINT\"]";
        DBObject object = new DBObject(
                Logger.getLogger(DBObjectTest.class),
                "test_table",
                allowedColumns,
                "id",
                "updated_at",
                "",
                1,
                "",
                "",
                0,
                "",
                new HashMap<>());

        String sql = object.getSelectTableSql();

        assertTrue(sql.contains("> '$1'"), "Expected numeric incremental key SQL to include a placeholder value, but was: " + sql);
    }

    @Test
    void postgresColumnDefinitionSynonymsAreTreatedAsEquivalent() {
        assertTrue(DBReader.areColumnDefinitionsEquivalent("VARCHAR(50) NULL", "CHARACTER VARYING(50) NULL"));
        assertTrue(DBReader.areColumnDefinitionsEquivalent("INT4", "INTEGER"));
        assertTrue(DBReader.areColumnDefinitionsEquivalent("\"name\" VARCHAR(255) NOT NULL", "name varchar(255) not null"));
    }

    private static void configureDefaults() throws Exception {
        ConfLoader loader = ConfLoader.getInstance();
        setField(loader, "srcType", SrcType.SQLITE);
        setField(loader, "srcDatabase", null);
        setField(loader, "srcSchema", null);
        setField(loader, "srcUseCatalogScopeResolution", Boolean.FALSE);
        setField(loader, "srcUseSchemaScopeResolution", Boolean.FALSE);
        setField(loader, "srcQuoteObjectNames", Boolean.FALSE);
        setField(loader, "srcQuoteColumnNames", Boolean.FALSE);
        setField(loader, "srcComputeMaxIncrementalKeyInDB", Boolean.TRUE);
        setField(loader, "srcReadNullIncrementalKeyRecords", Boolean.FALSE);
        setField(loader, "srcDBReaderObjectRecordLimit", 0L);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
