package org.opengauss.datachecker.extract.service;

import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.SQLException;
import java.util.Map;

@SpringBootTest
public class MetaDataServiceTest {

    @Autowired
    private MetaDataService metaDataService;

    @Test
    void queryMetadataOfSourceDBSchema() throws SQLException {
        Map<String, TableMetadata> stringTableMetadataMap = metaDataService.queryMetaDataOfSchema();
        for (TableMetadata metadata : stringTableMetadataMap.values()) {
            System.out.println(metadata.toString());
        }
    }
}
