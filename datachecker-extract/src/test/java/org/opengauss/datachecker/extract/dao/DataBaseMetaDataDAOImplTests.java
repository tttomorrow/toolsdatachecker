/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.extract.dao;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.SQLException;
import java.util.List;

/**
 * DataBaseMetaDataDAOImplTests
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/14
 * @since ：11
 */
@Slf4j
@SpringBootTest
class DataBaseMetaDataDAOImplTests {
    @Autowired
    private MetaDataDAO mysqlMetadataDAO;

    @Test
    void queryTableMetadata() throws SQLException {
        List<TableMetadata> tableMetadata = mysqlMetadataDAO.queryTableMetadata();
        for (TableMetadata metadata : tableMetadata) {
            log.info(metadata.toString());
        }
    }

    @Test
    void queryColumnMetadata() throws SQLException {
        List<TableMetadata> tableMetadata = mysqlMetadataDAO.queryTableMetadata();
        for (TableMetadata metadata : tableMetadata) {
            List<ColumnsMetaData> columnsMetadata = mysqlMetadataDAO.queryColumnMetadata(metadata.getTableName());
            for (ColumnsMetaData colMetadata : columnsMetadata) {
                log.info(colMetadata.toString());
            }
        }
    }
}
