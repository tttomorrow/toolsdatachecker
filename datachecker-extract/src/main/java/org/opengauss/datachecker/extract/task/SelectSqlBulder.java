package org.opengauss.datachecker.extract.task;

import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.opengauss.datachecker.extract.task.SelectSqlBulder.QuerySqlMapper.*;


/**
 * @author wang chao
 * @description 数据抽取SQL构建器
 * @date 2022/5/12 19:17
 * @since 11
 **/
public class SelectSqlBulder {
    private static final long OFF_SET_ZERO = 0L;
    /**
     * 任务执行起始位置
     */
    private final long start;
    /**
     * 任务执行偏移量
     */
    private final long offset;
    /**
     * 查询数据schema
     */
    private final String schema;
    /**
     * 表元数据信息
     */
    private final TableMetadata tableMetadata;

    public SelectSqlBulder(TableMetadata tableMetadata, String schema, long start, long offset) {
        this.tableMetadata = tableMetadata;
        this.start = start;
        this.offset = offset;
        this.schema = schema;
    }

    public String builder() {
        Assert.isTrue(Objects.nonNull(tableMetadata), "表元数据信息异常，构建SQL失败");
        List<ColumnsMetaData> columnsMetas = tableMetadata.getColumnsMetas();
        if (offset == OFF_SET_ZERO) {
            return buildSelectSqlOffsetZero(columnsMetas, tableMetadata.getTableName());
        } else {
            return buildSelectSqlOffset(tableMetadata, start, offset);
        }
    }

    /**
     * 根据元数据信息构建查询语句 SELECT * FROM test.test1
     *
     * @param columnsMetas 列元数据信息
     * @param tableName    表名
     * @return
     */
    private String buildSelectSqlOffsetZero(List<ColumnsMetaData> columnsMetas, String tableName) {
        String columnNames = columnsMetas
                .stream()
                .map(ColumnsMetaData::getColumnName)
                .collect(Collectors.joining(DELIMITER));
        return QUERY_OFF_SET_ZERO.replace(COLUMN, columnNames).replace(SCHEMA, schema).replace(TABLE_NAME, tableName);
    }

    /**
     * 根据元数据和分片信息构建查询语句
     * SELECT * FROM test.test1 WHERE b_number IN (SELECT t.b_number FROM (SELECT b_number FROM test.test1 LIMIT 0,20) t);
     *
     * @param tableMetadata 表元数据信息
     * @param start         分片查询起始位置
     * @param offset        分片查询位移
     * @return 返回构建的Select语句
     */
    //
    private String buildSelectSqlOffset(TableMetadata tableMetadata, long start, long offset) {
        List<ColumnsMetaData> columnsMetas = tableMetadata.getColumnsMetas();
        List<ColumnsMetaData> primaryMetas = tableMetadata.getPrimaryMetas();

        String columnNames;
        String primaryKey;
        String tableName = tableMetadata.getTableName();
        if (primaryMetas.size() == 1) {
            columnNames = columnsMetas
                    .stream()
                    .map(ColumnsMetaData::getColumnName)
                    .collect(Collectors.joining(DELIMITER));
            primaryKey = primaryMetas.stream().map(ColumnsMetaData::getColumnName)
                    .collect(Collectors.joining());
            return QUERY_OFF_SET.replace(COLUMN, columnNames)
                    .replace(SCHEMA, schema)
                    .replace(TABLE_NAME, tableName)
                    .replace(PRIMARY_KEY, primaryKey)
                    .replace(START, String.valueOf(start))
                    .replace(OFFSET, String.valueOf(offset));
        } else {
            columnNames = columnsMetas
                    .stream()
                    .map(ColumnsMetaData::getColumnName)
                    .map(counm -> TABLE_ALAIS.concat(counm))
                    .collect(Collectors.joining(DELIMITER));
            primaryKey = primaryMetas.stream().map(ColumnsMetaData::getColumnName)
                    .collect(Collectors.joining(DELIMITER));
            String joinOn = primaryMetas.stream()
                    .map(ColumnsMetaData::getColumnName)
                    .map(coumn -> TABLE_ALAIS.concat(coumn).concat(EQUAL_CONDITION).concat(SUB_TABLE_ALAIS).concat(coumn))
                    .collect(Collectors.joining(AND_CONDITION));
            return QUERY_MULTIPLE_PRIMARY_KEY_OFF_SET.replace(COLUMN, columnNames)
                    .replace(SCHEMA, schema)
                    .replace(TABLE_NAME, tableName)
                    .replace(PRIMARY_KEY, primaryKey)
                    .replace(JOIN_ON, joinOn)
                    .replace(START, String.valueOf(start))
                    .replace(OFFSET, String.valueOf(offset));
        }
    }

    /**
     * 查询SQL构建模版
     */
    interface QuerySqlMapper {
        /**
         * 表字段
         */
        String COLUMN = ":columnsList";

        /**
         * 表名称
         */
        String TABLE_NAME = ":tableName";

        /**
         * 表主键
         */
        String PRIMARY_KEY = ":primaryKey";
        String SCHEMA = ":schema";
        /**
         * 分片查询起始位置
         */
        String START = ":start";
        /**
         * 分片查询偏移量
         */
        String OFFSET = ":offset";
        String JOIN_ON = ":joinOn";
        /**
         * 无偏移量场景下，查询SQL语句
         */
        String QUERY_OFF_SET_ZERO = "SELECT :columnsList FROM :schema.:tableName";
        /**
         * 单一主键场景下，使用偏移量进行分片查询的SQL语句
         */
        String QUERY_OFF_SET = "SELECT :columnsList FROM :schema.:tableName WHERE :primaryKey IN (SELECT t.:primaryKey FROM (SELECT :primaryKey FROM :schema.:tableName LIMIT :start,:offset) t)";
        String QUERY_MULTIPLE_PRIMARY_KEY_OFF_SET = "SELECT :columnsList FROM :schema.:tableName a  RIGHT JOIN  (SELECT :primaryKey FROM :schema.:tableName LIMIT :start,:offset) b ON :joinOn";
        /**
         * SQL语句字段间隔符号
         */
        String DELIMITER = ",";
        /**
         * SQL语句 相等条件符号
         */
        String EQUAL_CONDITION = "=";
        String AND_CONDITION = " and ";
        /**
         * 表别名
         */
        String TABLE_ALAIS = "a.";
        /**
         * 子查询结果别名
         */
        String SUB_TABLE_ALAIS = "b.";
    }
}
