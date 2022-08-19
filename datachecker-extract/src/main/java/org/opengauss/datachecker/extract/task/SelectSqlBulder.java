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

import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.opengauss.datachecker.extract.task.SelectSqlBulder.QuerySqlMapper.AND_CONDITION;
import static org.opengauss.datachecker.extract.task.SelectSqlBulder.QuerySqlMapper.COLUMN;
import static org.opengauss.datachecker.extract.task.SelectSqlBulder.QuerySqlMapper.DELIMITER;
import static org.opengauss.datachecker.extract.task.SelectSqlBulder.QuerySqlMapper.EQUAL_CONDITION;
import static org.opengauss.datachecker.extract.task.SelectSqlBulder.QuerySqlMapper.JOIN_ON;
import static org.opengauss.datachecker.extract.task.SelectSqlBulder.QuerySqlMapper.OFFSET;
import static org.opengauss.datachecker.extract.task.SelectSqlBulder.QuerySqlMapper.PRIMARY_KEY;
import static org.opengauss.datachecker.extract.task.SelectSqlBulder.QuerySqlMapper.QUERY_MULTIPLE_PRIMARY_KEY_OFF_SET;
import static org.opengauss.datachecker.extract.task.SelectSqlBulder.QuerySqlMapper.QUERY_OFF_SET;
import static org.opengauss.datachecker.extract.task.SelectSqlBulder.QuerySqlMapper.QUERY_OFF_SET_ZERO;
import static org.opengauss.datachecker.extract.task.SelectSqlBulder.QuerySqlMapper.SCHEMA;
import static org.opengauss.datachecker.extract.task.SelectSqlBulder.QuerySqlMapper.START;
import static org.opengauss.datachecker.extract.task.SelectSqlBulder.QuerySqlMapper.SUB_TABLE_ALIAS;
import static org.opengauss.datachecker.extract.task.SelectSqlBulder.QuerySqlMapper.TABLE_ALIAS;
import static org.opengauss.datachecker.extract.task.SelectSqlBulder.QuerySqlMapper.TABLE_NAME;

/**
 * Data extraction SQL builder
 *
 * @author wang chao
 * @date 2022/5/12 19:17
 * @since 11
 **/
public class SelectSqlBulder {
    private static final long OFF_SET_ZERO = 0L;
    /**
     * Start position of task execution
     */
    private final long start;
    /**
     * Task execution offset
     */
    private final long offset;
    /**
     * Query data schema
     */
    private final String schema;
    /**
     * Table metadata information
     */
    private final TableMetadata tableMetadata;

    /**
     * Table fragment query SQL Statement Builder
     *
     * @param tableMetadata tableMetadata
     * @param schema        schema
     * @param start         start
     * @param offset        offset
     */
    public SelectSqlBulder(TableMetadata tableMetadata, String schema, long start, long offset) {
        this.tableMetadata = tableMetadata;
        this.start = start;
        this.offset = offset;
        this.schema = schema;
    }

    /**
     * Table fragment query SQL Statement Builder
     *
     * @return build sql
     */
    public String builder() {
        Assert.isTrue(Objects.nonNull(tableMetadata), "Abnormal table metadata information, failed to build SQL");
        List<ColumnsMetaData> columnsMetas = tableMetadata.getColumnsMetas();
        if (offset == OFF_SET_ZERO) {
            return buildSelectSqlOffsetZero(columnsMetas, tableMetadata.getTableName());
        } else {
            return buildSelectSqlOffset(tableMetadata, start, offset);
        }
    }

    /**
     * Construct query statements based on metadata information SELECT * FROM test.test1
     *
     * @param columnsMetas Column metadata information
     * @param tableName    tableName
     * @return
     */
    private String buildSelectSqlOffsetZero(List<ColumnsMetaData> columnsMetas, String tableName) {
        String columnNames =
            columnsMetas.stream().map(ColumnsMetaData::getColumnName).collect(Collectors.joining(DELIMITER));
        return QUERY_OFF_SET_ZERO.replace(COLUMN, columnNames).replace(SCHEMA, schema).replace(TABLE_NAME, tableName);
    }

    /**
     * <pre>
     * Construct query statements based on metadata and fragment information
     * SELECT * FROM test.test1 WHERE b_number IN
     * (SELECT t.b_number FROM (SELECT b_number FROM test.test1 LIMIT 0,20) t);
     * </pre>
     *
     * @param tableMetadata Table metadata information
     * @param start         Start position of fragment query
     * @param offset        Fragment query start position fragment query displacement
     * @return Return the constructed select statement
     */
    private String buildSelectSqlOffset(TableMetadata tableMetadata, long start, long offset) {
        List<ColumnsMetaData> columnsMetas = tableMetadata.getColumnsMetas();
        List<ColumnsMetaData> primaryMetas = tableMetadata.getPrimaryMetas();
        String columnNames;
        String primaryKey;
        String tableName = tableMetadata.getTableName();
        if (primaryMetas.size() == 1) {
            columnNames =
                columnsMetas.stream().map(ColumnsMetaData::getColumnName).collect(Collectors.joining(DELIMITER));
            primaryKey = primaryMetas.stream().map(ColumnsMetaData::getColumnName).collect(Collectors.joining());
            return QUERY_OFF_SET.replace(COLUMN, columnNames).replace(SCHEMA, schema).replace(TABLE_NAME, tableName)
                                .replace(PRIMARY_KEY, primaryKey).replace(START, String.valueOf(start))
                                .replace(OFFSET, String.valueOf(offset));
        } else {
            columnNames =
                columnsMetas.stream().map(ColumnsMetaData::getColumnName).map(counm -> TABLE_ALIAS.concat(counm))
                            .collect(Collectors.joining(DELIMITER));
            primaryKey =
                primaryMetas.stream().map(ColumnsMetaData::getColumnName).collect(Collectors.joining(DELIMITER));
            String joinOn = primaryMetas.stream().map(ColumnsMetaData::getColumnName).map(
                coumn -> TABLE_ALIAS.concat(coumn).concat(EQUAL_CONDITION).concat(SUB_TABLE_ALIAS).concat(coumn))
                                        .collect(Collectors.joining(AND_CONDITION));
            return QUERY_MULTIPLE_PRIMARY_KEY_OFF_SET.replace(COLUMN, columnNames).replace(SCHEMA, schema)
                                                     .replace(TABLE_NAME, tableName).replace(PRIMARY_KEY, primaryKey)
                                                     .replace(JOIN_ON, joinOn).replace(START, String.valueOf(start))
                                                     .replace(OFFSET, String.valueOf(offset));
        }
    }

    /**
     * Query SQL build template
     */
    interface QuerySqlMapper {
        /**
         * Query SQL statement columnsList fragment
         */
        String COLUMN = ":columnsList";
        /**
         * Query SQL statement tableName fragment
         */
        String TABLE_NAME = ":tableName";
        /**
         * Query SQL statement primaryKey fragment
         */
        String PRIMARY_KEY = ":primaryKey";
        /**
         * Query SQL statement schema fragment
         */
        String SCHEMA = ":schema";
        /**
         * Query SQL statement start fragment: Start position of fragment query
         */
        String START = ":start";
        /**
         * Query SQL statement offset fragment: Fragment query offset
         */
        String OFFSET = ":offset";
        /**
         * Query SQL statement joinOn fragment: Query SQL statement joinOn fragment
         */
        String JOIN_ON = ":joinOn";
        /**
         * Query SQL statement fragment: Query SQL statements in the scenario without offset
         */
        String QUERY_OFF_SET_ZERO = "SELECT :columnsList FROM :schema.:tableName";
        /**
         * Query SQL statement fragment: SQL statement for fragment query using offset in single primary key scenario
         */
        String QUERY_OFF_SET = "SELECT :columnsList FROM :schema.:tableName WHERE :primaryKey IN "
            + "(SELECT t.:primaryKey FROM (SELECT :primaryKey FROM :schema.:tableName order by :primaryKey "
            + " LIMIT :start,:offset) t)";
        /**
         * Query SQL statement fragment: SQL statement for fragment query using offset in multiple primary key scenario
         */
        String QUERY_MULTIPLE_PRIMARY_KEY_OFF_SET = "SELECT :columnsList FROM :schema.:tableName a  RIGHT JOIN "
            + " (SELECT :primaryKey FROM :schema.:tableName order by :primaryKey LIMIT :start,:offset) b ON :joinOn";
        /**
         * Query SQL statement fragment: SQL statement field spacing symbol
         */
        String DELIMITER = ",";
        /**
         * Query SQL statement fragment: SQL statement equality condition symbol
         */
        String EQUAL_CONDITION = "=";
        /**
         * Query SQL statement and fragment
         */
        String AND_CONDITION = " and ";
        /**
         * Query SQL statement table alias fragment: table alias
         */
        String TABLE_ALIAS = "a.";
        /**
         * Query SQL statement sub table alias fragment:  Sub query result alias
         */
        String SUB_TABLE_ALIAS = "b.";
    }
}
