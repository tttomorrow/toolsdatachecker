package org.opengauss.datachecker.extract.dml;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SelectDmlBuilderTest {

    private SelectDmlBuilder selectDmlBuilderMysql;
    private SelectDmlBuilder selectDmlBuilderOpenGauss;

    @BeforeEach
    void setUp() {
        selectDmlBuilderMysql = new SelectDmlBuilder(DataBaseType.MS);
        selectDmlBuilderOpenGauss = new SelectDmlBuilder(DataBaseType.OG);
    }

    @DisplayName("test build columns")
    @Test
    void testColumns() {
        // Setup
        final ColumnsMetaData columnsMetaData = new ColumnsMetaData();
        columnsMetaData.setTableName("tableName");
        columnsMetaData.setColumnName("columnName");
        columnsMetaData.setColumnType("columnType");
        columnsMetaData.setDataType("dataType");
        columnsMetaData.setOrdinalPosition(0);
        columnsMetaData.setColumnKey(ColumnKey.PRI);
        final List<ColumnsMetaData> columnsMetas = List.of(columnsMetaData);

        // Run the test
        final SelectDmlBuilder result = selectDmlBuilderMysql.columns(columnsMetas);
        final SelectDmlBuilder resultOg = selectDmlBuilderOpenGauss.columns(columnsMetas);
        // Verify the results
        assertThat(result.columns).isEqualTo("`columnName`");
        assertThat(resultOg.columns).isEqualTo("\"columnName\"");
    }

    @DisplayName("test build multiple columns")
    @Test
    void testBuildMultipleColumns() {
        // Setup
        final ColumnsMetaData columnsMetaData = new ColumnsMetaData();
        columnsMetaData.setTableName("tableName");
        columnsMetaData.setColumnName("columnName");
        columnsMetaData.setColumnType("columnType");
        columnsMetaData.setDataType("dataType");
        columnsMetaData.setOrdinalPosition(0);
        columnsMetaData.setColumnKey(ColumnKey.PRI);
        final ColumnsMetaData columnsMetaData2 = new ColumnsMetaData();
        columnsMetaData2.setTableName("tableName");
        columnsMetaData2.setColumnName("columnName2");
        columnsMetaData2.setColumnType("columnType2");
        columnsMetaData2.setDataType("dataType2");
        columnsMetaData2.setOrdinalPosition(1);
        final List<ColumnsMetaData> columnsMetas = List.of(columnsMetaData, columnsMetaData2);

        // Run the test
        final SelectDmlBuilder result = selectDmlBuilderMysql.columns(columnsMetas);
        final SelectDmlBuilder resultOg = selectDmlBuilderOpenGauss.columns(columnsMetas);
        assertThat(result.columns).isEqualTo("`columnName`,`columnName2`");
        assertThat(resultOg.columns).isEqualTo("\"columnName\",\"columnName2\"");
    }

    @DisplayName("test build database type")
    @Test
    void testDataBaseType() {
        // Setup
        // Run the test
        final SelectDmlBuilder result = selectDmlBuilderMysql.dataBaseType(DataBaseType.MS);
        final SelectDmlBuilder resultOg = selectDmlBuilderOpenGauss.dataBaseType(DataBaseType.OG);
        assertThat(result.dataBaseType).isEqualTo(DataBaseType.MS);
        assertThat(resultOg.dataBaseType).isEqualTo(DataBaseType.OG);
    }

    @DisplayName("test build database schema")
    @Test
    void testSchema() {
        // Setup
        // Run the test
        final SelectDmlBuilder result = selectDmlBuilderMysql.schema("schema");
        final SelectDmlBuilder resultOg = selectDmlBuilderOpenGauss.schema("schema");

        assertThat(result.schema).isEqualTo("`schema`");
        assertThat(resultOg.schema).isEqualTo("\"schema\"");
    }

    @DisplayName("test build condition primary")
    @Test
    void testConditionPrimary() {
        // Setup
        final ColumnsMetaData primaryMeta = new ColumnsMetaData();
        primaryMeta.setTableName("tableName");
        primaryMeta.setColumnName("columnName");
        primaryMeta.setColumnType("columnType");
        primaryMeta.setDataType("dataType");
        primaryMeta.setOrdinalPosition(0);
        primaryMeta.setColumnKey(ColumnKey.PRI);

        // Run the test
        final SelectDmlBuilder result = selectDmlBuilderMysql.conditionPrimary(primaryMeta);
        final SelectDmlBuilder resultOg = selectDmlBuilderOpenGauss.conditionPrimary(primaryMeta);

        assertThat(result.condition).isEqualTo("columnName in ( :primaryKeys )");
        assertThat(resultOg.condition).isEqualTo("columnName in ( :primaryKeys )");
    }

    @DisplayName("test build condition composite primary")
    @Test
    void testConditionCompositePrimary() {
        // Setup
        final ColumnsMetaData columnsMetaData = new ColumnsMetaData();
        columnsMetaData.setTableName("tableName");
        columnsMetaData.setColumnName("columnName");
        columnsMetaData.setColumnType("columnType");
        columnsMetaData.setDataType("dataType");
        columnsMetaData.setOrdinalPosition(0);
        columnsMetaData.setColumnKey(ColumnKey.PRI);
        final ColumnsMetaData columnsMetaData2 = new ColumnsMetaData();
        columnsMetaData2.setTableName("tableName");
        columnsMetaData2.setColumnName("columnName2");
        columnsMetaData2.setColumnType("columnType");
        columnsMetaData2.setDataType("dataType");
        columnsMetaData2.setOrdinalPosition(1);
        columnsMetaData2.setColumnKey(ColumnKey.PRI);
        final List<ColumnsMetaData> primaryMeta = List.of(columnsMetaData, columnsMetaData2);

        // Run the test
        final SelectDmlBuilder result = selectDmlBuilderMysql.conditionCompositePrimary(primaryMeta);
        final SelectDmlBuilder resultOg = selectDmlBuilderOpenGauss.conditionCompositePrimary(primaryMeta);

        assertThat(result.condition).isEqualTo("(`columnName`,`columnName2`) in ( :primaryKeys )");
        assertThat(resultOg.condition).isEqualTo("(\"columnName\",\"columnName2\") in ( :primaryKeys )");
    }

    @DisplayName("test build condition composite primary values")
    @Test
    void testConditionCompositePrimaryValue() {
        // Setup
        final ColumnsMetaData columnsMetaData = new ColumnsMetaData();
        columnsMetaData.setTableName("tableName");
        columnsMetaData.setColumnName("columnName");
        columnsMetaData.setColumnType("columnType");
        columnsMetaData.setDataType("dataType");
        columnsMetaData.setOrdinalPosition(0);
        columnsMetaData.setColumnKey(ColumnKey.PRI);
        final ColumnsMetaData columnsMetaData2 = new ColumnsMetaData();
        columnsMetaData2.setTableName("tableName");
        columnsMetaData2.setColumnName("columnName2");
        columnsMetaData2.setColumnType("columnType");
        columnsMetaData2.setDataType("dataType");
        columnsMetaData2.setOrdinalPosition(1);
        columnsMetaData2.setColumnKey(ColumnKey.PRI);
        final List<ColumnsMetaData> primaryMeta = List.of(columnsMetaData, columnsMetaData2);

        // Run the test
        final List<Object[]> result =
            selectDmlBuilderMysql.conditionCompositePrimaryValue(primaryMeta, List.of("value", "value2"));
        System.out.println(result);
        // Verify the results
    }

    @DisplayName("test build table")
    @Test
    void testTableName() {
        // Setup
        // Run the test
        final SelectDmlBuilder result = selectDmlBuilderMysql.tableName("tableName");
        final SelectDmlBuilder resultOg = selectDmlBuilderOpenGauss.tableName("tableName");

        assertThat(result.tableName).isEqualTo("`tableName`");
        assertThat(resultOg.tableName).isEqualTo("\"tableName\"");
    }

    @DisplayName("test build select sql condition primary ")
    @Test
    void testBuild() {
        // Setup
        final ColumnsMetaData columnsMetaData = new ColumnsMetaData();
        columnsMetaData.setTableName("tableName");
        columnsMetaData.setColumnName("columnName");
        columnsMetaData.setColumnType("columnType");
        columnsMetaData.setDataType("dataType");
        columnsMetaData.setOrdinalPosition(0);
        columnsMetaData.setColumnKey(ColumnKey.PRI);
        final ColumnsMetaData columnsMetaData2 = new ColumnsMetaData();
        columnsMetaData2.setTableName("tableName");
        columnsMetaData2.setColumnName("columnName2");
        columnsMetaData2.setColumnType("columnType");
        columnsMetaData2.setDataType("dataType");
        columnsMetaData2.setOrdinalPosition(1);
        final List<ColumnsMetaData> columnMetas = List.of(columnsMetaData, columnsMetaData2);
        selectDmlBuilderMysql.schema("schema").tableName("table").columns(columnMetas)
                             .conditionPrimary(columnsMetaData);
        assertThat(selectDmlBuilderMysql.build())
            .isEqualTo("select `columnName`,`columnName2` from `schema`.`table` where columnName in ( :primaryKeys );");
    }

    @DisplayName("test build select sql condition composite primary ")
    @Test
    void testBuild3() {
        // Setup
        final ColumnsMetaData columnsMetaData = new ColumnsMetaData();
        columnsMetaData.setTableName("tableName");
        columnsMetaData.setColumnName("columnName");
        columnsMetaData.setColumnType("columnType");
        columnsMetaData.setDataType("dataType");
        columnsMetaData.setOrdinalPosition(0);
        columnsMetaData.setColumnKey(ColumnKey.PRI);
        final ColumnsMetaData columnsMetaData2 = new ColumnsMetaData();
        columnsMetaData2.setTableName("tableName");
        columnsMetaData2.setColumnName("columnName2");
        columnsMetaData2.setColumnType("columnType");
        columnsMetaData2.setDataType("dataType");
        columnsMetaData2.setOrdinalPosition(1);
        columnsMetaData2.setColumnKey(ColumnKey.PRI);
        final List<ColumnsMetaData> columnMetas = List.of(columnsMetaData, columnsMetaData2);
        selectDmlBuilderMysql.schema("schema").tableName("table").columns(columnMetas)
                             .conditionCompositePrimary(columnMetas);
        assertThat(selectDmlBuilderMysql.build())
            .isEqualTo("select `columnName`,`columnName2` from `schema`.`table` where (`columnName`,`columnName2`) in ( :primaryKeys );");
    }
}
