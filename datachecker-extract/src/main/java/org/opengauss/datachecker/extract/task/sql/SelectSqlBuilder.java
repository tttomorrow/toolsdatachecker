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

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.COLUMN;
import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.DELIMITER;
import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.OFFSET;
import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.SCHEMA;
import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.START;
import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.TABLE_ALIAS;
import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.TABLE_NAME;

/**
 * OpenGaussSelectSqlBuilder  Data extraction SQL builder
 *
 * @author wang chao
 * @date 2022/5/12 19:17
 * @since 11
 **/
public class SelectSqlBuilder {
    private static final Map<DataBaseType, SqlGenerate> SQL_GENERATE = new HashMap<>();
    private static final long OFF_SET_ZERO = 0L;
    private static final SqlGenerateTemplate GENERATE_TEMPLATE =
        (template, sqlGenerateMeta) -> template.replace(COLUMN, sqlGenerateMeta.getColumns())
                                               .replace(SCHEMA, sqlGenerateMeta.getSchema())
                                               .replace(TABLE_NAME, sqlGenerateMeta.getTableName())
                                               .replace(START, String.valueOf(sqlGenerateMeta.getStart()))
                                               .replace(OFFSET, String.valueOf(sqlGenerateMeta.getOffset()));
    private static final SqlGenerateTemplate NO_OFFSET_SQL_GENERATE_TEMPLATE =
        (template, sqlGenerateMeta) -> template.replace(COLUMN, sqlGenerateMeta.getColumns())
                                               .replace(SCHEMA, sqlGenerateMeta.getSchema())
                                               .replace(TABLE_NAME, sqlGenerateMeta.getTableName());
    private static final SqlGenerate OFFSET_GENERATE =
        (sqlGenerateMeta) -> GENERATE_TEMPLATE.replace(QuerySqlTemplate.QUERY_OFF_SET, sqlGenerateMeta);
    private static final SqlGenerate NO_OFFSET_GENERATE = (sqlGenerateMeta) -> NO_OFFSET_SQL_GENERATE_TEMPLATE
        .replace(QuerySqlTemplate.QUERY_NO_OFF_SET, sqlGenerateMeta);

    static {
        SQL_GENERATE.put(DataBaseType.MS, OFFSET_GENERATE);
        SQL_GENERATE.put(DataBaseType.OG, OFFSET_GENERATE);
        SQL_GENERATE.put(DataBaseType.O, OFFSET_GENERATE);
    }

    private String schema;
    private TableMetadata tableMetadata;
    private long start = 0L;
    private long offset = 0L;
    private DataBaseType dataBaseType;
    private boolean isDivisions;

    /**
     * Table fragment query SQL Statement Builder
     *
     * @param tableMetadata tableMetadata
     * @param schema        schema
     */
    public SelectSqlBuilder(TableMetadata tableMetadata, String schema) {
        this.tableMetadata = tableMetadata;
        this.schema = schema;
    }

    /**
     * Table fragment query SQL Statement Builder
     *
     * @param start  start
     * @param offset offset
     * @return builder
     */
    public SelectSqlBuilder offset(long start, long offset) {
        this.start = start;
        this.offset = offset;
        return this;
    }

    /**
     * current table query sql is divisions
     *
     * @param isDivisions isDivisions
     * @return builder
     */
    public SelectSqlBuilder isDivisions(boolean isDivisions) {
        this.isDivisions = isDivisions;
        return this;
    }

    /**
     * set param dataBaseType
     *
     * @param dataBaseType dataBaseType
     * @return builder
     */
    public SelectSqlBuilder dataBaseType(DataBaseType dataBaseType) {
        this.dataBaseType = dataBaseType;
        return this;
    }

    /**
     * Table fragment query SQL Statement Builder
     *
     * @return build sql
     */
    public String builder() {
        Assert.isTrue(Objects.nonNull(tableMetadata), Message.TABLE_METADATA_NULL_NOT_TO_BUILD_SQL);
        List<ColumnsMetaData> columnsMetas = tableMetadata.getColumnsMetas();
        Assert.notEmpty(columnsMetas, Message.COLUMN_METADATA_EMPTY_NOT_TO_BUILD_SQL);
        if (offset == OFF_SET_ZERO || !isDivisions) {
            return buildSelectSqlOffsetZero(columnsMetas, tableMetadata.getTableName());
        } else {
            return buildSelectSqlOffset(tableMetadata, start, offset);
        }
    }

    /**
     * <pre>
     * Construct query statements based on metadata and fragment information
     * SELECT * FROM test.test1 LIMIT 0,20
     * </pre>
     *
     * @param tableMetadata Table metadata information
     * @param start         Start position of fragment query
     * @param offset        Fragment query start position fragment query displacement
     * @return Return the constructed select statement
     */
    public String buildSelectSqlOffset(TableMetadata tableMetadata, long start, long offset) {
        List<ColumnsMetaData> columnsMetas = tableMetadata.getColumnsMetas();
        String tableName = tableMetadata.getTableName();
        String columnNames = getColumnNameList(columnsMetas);
        SqlGenerateMeta sqlGenerateMeta = new SqlGenerateMeta(schema, tableName, columnNames, start, offset);
        return getSqlGenerate(dataBaseType).replace(sqlGenerateMeta);
    }

    /**
     * Construct query statements based on metadata information SELECT * FROM test.test1
     *
     * @param columnsMetas Column metadata information
     * @param tableName    tableName
     * @return sql
     */
    private String buildSelectSqlOffsetZero(List<ColumnsMetaData> columnsMetas, String tableName) {
        String columnNames = getColumnNameList(columnsMetas);
        SqlGenerateMeta sqlGenerateMeta = new SqlGenerateMeta(schema, tableName, columnNames, 0, 0);
        return NO_OFFSET_GENERATE.replace(sqlGenerateMeta);
    }

    private static String getColumnNameList(@NonNull List<ColumnsMetaData> columnsMetas) {
        return columnsMetas.stream().map(ColumnsMetaData::getColumnName).collect(Collectors.joining(DELIMITER));
    }

    /**
     * get column names with alias
     *
     * @param columnsMetas columnsMetas
     * @return column names
     */
    private String getColumnNameListWithAlias(@NonNull List<ColumnsMetaData> columnsMetas) {
        return columnsMetas.stream().map(ColumnsMetaData::getColumnName).map(TABLE_ALIAS::concat)
                           .collect(Collectors.joining(DELIMITER));
    }

    private SqlGenerate getSqlGenerate(DataBaseType dataBaseType) {
        return SQL_GENERATE.get(dataBaseType);
    }

    @Getter
    @AllArgsConstructor
    static class SqlGenerateMeta {
        private final String schema;
        private final String tableName;
        private final String columns;
        private final long start;
        private final long offset;
    }

    @FunctionalInterface
    interface SqlGenerate {
        /**
         * Generate SQL statement according to SQL generator metadata object
         *
         * @param sqlGenerateMeta SQL generator metadata
         * @return Return fragment query SQL statement
         */
        String replace(SqlGenerateMeta sqlGenerateMeta);
    }

    @FunctionalInterface
    interface SqlGenerateTemplate {
        /**
         * Generate SQL statement according to SQL generator metadata object
         *
         * @param template        SQL template
         * @param sqlGenerateMeta SQL generator metadata
         * @return sql
         */
        String replace(String template, SqlGenerateMeta sqlGenerateMeta);
    }

    interface Message {
        /**
         * error message tips
         */
        String TABLE_METADATA_NULL_NOT_TO_BUILD_SQL = "Abnormal table metadata information, failed to build SQL";

        /**
         * error message tips
         */
        String COLUMN_METADATA_EMPTY_NOT_TO_BUILD_SQL = "Abnormal column metadata information, failed to build SQL";
    }
}
