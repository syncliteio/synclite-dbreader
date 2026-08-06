package com.synclite.dbreader;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ConfLoaderTest {

    @Test
    void developerEditionAllowsMySqlSource() {
        assertTrue(
                ConfLoader.isSourceSupportedInDeveloperEdition(SrcType.MYSQL),
                "MySQL source must be available in the Developer edition");
    }
}
