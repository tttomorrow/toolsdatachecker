package org.opengauss.datachecker.extract.dao;

import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.SQLException;
import java.util.List;

@SpringBootTest
class DataBaseMetaDataDAOImplTests {

    @Autowired
    private MetaDataDAO mysqlMetadataDAO;

    @Test
    void queryTableMetadata() throws SQLException {
        List<TableMetadata> tableMetadata = mysqlMetadataDAO.queryTableMetadata();
        for (TableMetadata metadata : tableMetadata) {
            System.out.println(metadata.toString());
        }
    }

    @Test
    void queryColumnMetadata() throws SQLException {
        List<TableMetadata> tableMetadata = mysqlMetadataDAO.queryTableMetadata();
        for (TableMetadata metadata : tableMetadata) {
            List<ColumnsMetaData> columnsMetadata = mysqlMetadataDAO.queryColumnMetadata(metadata.getTableName());
            for (ColumnsMetaData colMetadata : columnsMetadata) {
                System.out.println(colMetadata.toString());
            }
        }

    }
}
