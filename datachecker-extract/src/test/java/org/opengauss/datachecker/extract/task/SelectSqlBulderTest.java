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

package org.opengauss.datachecker.extract.task;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * SelectSqlBulderTest
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/14
 * @since ：11
 */
@ExtendWith(MockitoExtension.class)
class SelectSqlBulderTest {
    @Mock
    private TableMetadata mockTableMetadata;

    private SelectSqlBulder selectSqlBulderUnderTest;

    @BeforeEach
    void setUp() {
        selectSqlBulderUnderTest = new SelectSqlBulder(mockTableMetadata, "test", 0L, 0L);
    }

    @Test
    void testBuilder() {
        // Configure TableMetadata.getColumnsMetas(...).
        final ColumnsMetaData columnsMeta1 = new ColumnsMetaData();
        columnsMeta1.setTableName("tableName");
        columnsMeta1.setColumnName("columnName1");
        columnsMeta1.setColumnType("columnType");
        columnsMeta1.setDataType("dataType");
        columnsMeta1.setOrdinalPosition(2);
        final List<ColumnsMetaData> columnsMetaData = List.of(columnsMeta1);
        when(mockTableMetadata.getColumnsMetas()).thenReturn(columnsMetaData);
        when(mockTableMetadata.getTableName()).thenReturn("tableName");
        // Run the test
        final String result = selectSqlBulderUnderTest.builder();
        // Verify the results
        assertThat(result).isEqualTo("SELECT columnName1 FROM test.tableName");
    }

    @Test
    void testBuilderOffSet() {
        selectSqlBulderUnderTest = new SelectSqlBulder(mockTableMetadata, "test", 0L, 1000L);
        // Setup
        // Configure TableMetadata.getPrimaryMetas(...).
        final ColumnsMetaData columnsMetaPri = new ColumnsMetaData();
        columnsMetaPri.setTableName("tableName");
        columnsMetaPri.setColumnName("columnName1");
        columnsMetaPri.setColumnType("columnType");
        columnsMetaPri.setDataType("dataType");
        columnsMetaPri.setOrdinalPosition(1);
        columnsMetaPri.setColumnKey(ColumnKey.PRI);
        final List<ColumnsMetaData> primaryList = List.of(columnsMetaPri);
        when(mockTableMetadata.getPrimaryMetas()).thenReturn(primaryList);
        // Configure TableMetadata.getColumnsMetas(...).
        final ColumnsMetaData columnsMeta1 = new ColumnsMetaData();
        columnsMeta1.setTableName("tableName");
        columnsMeta1.setColumnName("columnName2");
        columnsMeta1.setColumnType("columnType");
        columnsMeta1.setDataType("dataType");
        columnsMeta1.setOrdinalPosition(2);
        final List<ColumnsMetaData> columnsMetaData = List.of(columnsMetaPri, columnsMeta1);
        when(mockTableMetadata.getColumnsMetas()).thenReturn(columnsMetaData);
        when(mockTableMetadata.getTableName()).thenReturn("tableName");
        // Run the test
        final String result = selectSqlBulderUnderTest.builder();
        // Verify the results
        assertThat(result).isEqualTo("SELECT columnName1,columnName2 FROM test.tableName WHERE columnName1 IN "
            + "(SELECT t.columnName1 FROM (SELECT columnName1 FROM test.tableName order by columnName1"
            + "  LIMIT 0,1000) t)");
    }

    @Test
    void testBuilderMuliPrimaryLeyOffSet() {
        selectSqlBulderUnderTest = new SelectSqlBulder(mockTableMetadata, "test", 0L, 1000L);
        // Setup
        // Configure TableMetadata.getPrimaryMetas(...).
        final ColumnsMetaData columnsMetaPri = new ColumnsMetaData();
        columnsMetaPri.setTableName("tableName");
        columnsMetaPri.setColumnName("columnName1");
        columnsMetaPri.setColumnType("columnType");
        columnsMetaPri.setDataType("dataType");
        columnsMetaPri.setOrdinalPosition(1);
        columnsMetaPri.setColumnKey(ColumnKey.PRI);

        // Configure TableMetadata.getColumnsMetas(...).
        final ColumnsMetaData columnsMeta1 = new ColumnsMetaData();
        columnsMeta1.setTableName("tableName");
        columnsMeta1.setColumnName("columnName2");
        columnsMeta1.setColumnType("columnType");
        columnsMeta1.setDataType("dataType");
        columnsMeta1.setOrdinalPosition(2);
        columnsMeta1.setColumnKey(ColumnKey.PRI);
        final List<ColumnsMetaData> columnsMetaData = List.of(columnsMetaPri, columnsMeta1);
        final List<ColumnsMetaData> primaryList = List.of(columnsMetaPri, columnsMeta1);
        when(mockTableMetadata.getPrimaryMetas()).thenReturn(primaryList);
        when(mockTableMetadata.getColumnsMetas()).thenReturn(columnsMetaData);
        when(mockTableMetadata.getTableName()).thenReturn("tableName");
        // Run the test
        final String result = selectSqlBulderUnderTest.builder();
        // Verify the results
        assertThat(result).isEqualTo("SELECT a.columnName1,a.columnName2 FROM test.tableName a  RIGHT JOIN "
            + " (SELECT columnName1,columnName2 FROM test.tableName order by columnName1,columnName2 LIMIT 0,1000) b"
            + " ON a.columnName1=b.columnName1 and a.columnName2=b.columnName2");
    }
}
