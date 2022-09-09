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

package org.opengauss.datachecker.extract.task.sql;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.task.sql.SelectSqlBuilder.Message;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.when;

/**
 * SelectSqlBuilderTest
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/2
 * @since ：11
 */
@ExtendWith(MockitoExtension.class)
class SelectSqlBuilderTest {
    @Mock
    private TableMetadata mockTableMetadata;
    private SelectSqlBuilder selectSqlBuilder;

    @BeforeEach
    void setUp() {
        selectSqlBuilder = new SelectSqlBuilder(mockTableMetadata, "schema");
    }

    /**
     * testBuilder
     */
    @Test
    void testBuilder() {
        // Setup
        // Configure TableMetadata.getColumnsMetas(...).
        final ColumnsMetaData columnsMetaData1 = new ColumnsMetaData();
        columnsMetaData1.setTableName("tableName");
        columnsMetaData1.setColumnName("columnName");
        columnsMetaData1.setColumnType("columnType");
        columnsMetaData1.setDataType("dataType");
        columnsMetaData1.setOrdinalPosition(0);
        columnsMetaData1.setColumnKey(ColumnKey.PRI);
        final List<ColumnsMetaData> columnsMetaData = List.of(columnsMetaData1);
        when(mockTableMetadata.getColumnsMetas()).thenReturn(columnsMetaData);

        when(mockTableMetadata.getTableName()).thenReturn("tableName");
        // Run the test
        final String result = selectSqlBuilder.dataBaseType(DataBaseType.MS).offset(0, 120).builder();

        // Verify the results
        assertThat(result).isEqualTo("SELECT columnName FROM schema.tableName LIMIT 0,120");
    }

    /**
     * testBuilder_TableMetadataGetColumnsMetasReturnsNoItems
     */
    @Test
    void testBuilder_TableMetadataGetColumnsMetasReturnsNoItems() {
        // Run the test
        assertThatThrownBy(() -> selectSqlBuilder.builder()).isInstanceOf(IllegalArgumentException.class)
                                                            .hasMessageContaining(
                                                                Message.COLUMN_METADATA_EMPTY_NOT_TO_BUILD_SQL);
    }

    /**
     * testBuildSelectSqlOffset
     */
    @Test
    void testBuildSelectSqlOffset() {
        // Setup
        final TableMetadata tableMetadata = new TableMetadata();
        tableMetadata.setTableName("tableName");
        tableMetadata.setTableRows(0L);
        final ColumnsMetaData columnsMetaData = new ColumnsMetaData();
        columnsMetaData.setTableName("tableName");
        columnsMetaData.setColumnName("pk_columnName1");
        columnsMetaData.setColumnType("pk_columnType1");
        columnsMetaData.setDataType("dataType");
        columnsMetaData.setOrdinalPosition(0);
        columnsMetaData.setColumnKey(ColumnKey.PRI);
        tableMetadata.setPrimaryMetas(List.of(columnsMetaData));
        final ColumnsMetaData columnsMetaData1 = new ColumnsMetaData();
        columnsMetaData1.setTableName("tableName");
        columnsMetaData1.setColumnName("columnName2");
        columnsMetaData1.setColumnType("columnType2");
        columnsMetaData1.setDataType("dataType");
        columnsMetaData1.setOrdinalPosition(0);
        columnsMetaData1.setColumnKey(ColumnKey.PRI);
        tableMetadata.setColumnsMetas(List.of(columnsMetaData, columnsMetaData1));

        // Run the test
        final String result =
            selectSqlBuilder.dataBaseType(DataBaseType.MS).offset(0, 120).buildSelectSqlOffset(tableMetadata, 0L, 100L);

        // Verify the results
        assertThat(result).isEqualTo("SELECT pk_columnName1,columnName2 FROM schema.tableName LIMIT 0,100");
    }
}
