package org.opengauss.datachecker.extract.task;

import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.TableMetadataHash;
import org.opengauss.datachecker.common.util.HashUtil;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.constants.ExtConstants;
import org.opengauss.datachecker.extract.dml.*;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.sql.ResultSetMetaData;
import java.util.*;
import java.util.stream.Collectors;

/**
 * DML  数据操作服务 实现数据的动态查询
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/13
 * @since ：11
 */
@Service
public class DataManipulationService {

    @Autowired
    private JdbcTemplate jdbcTemplateOne;
    @Autowired
    private MetaDataService metaDataService;
    @Autowired
    private ExtractProperties extractProperties;

    public List<Map<String, String>> queryColumnValues(String tableName, List<String> compositeKeys, TableMetadata metadata) {
        Assert.isTrue(Objects.nonNull(metadata), "表元数据信息异常，构建Select SQL失败");
        final List<ColumnsMetaData> primaryMetas = metadata.getPrimaryMetas();

        Assert.isTrue(!CollectionUtils.isEmpty(primaryMetas), "表主键元数据信息异常，构建Select SQL失败");

        // 单一主键表数据查询
        if (primaryMetas.size() == 1) {
            final ColumnsMetaData primaryData = primaryMetas.get(0);
            String querySql = new SelectDmlBuilder()
                    .schema(extractProperties.getSchema())
                    .columns(metadata.getColumnsMetas())
                    .tableName(tableName)
                    .conditionPrimary(primaryData)
                    .build();
            return queryColumnValues(querySql, compositeKeys);
        } else {
            // 复合主键表数据查询
            final SelectDmlBuilder dmlBuilder = new SelectDmlBuilder();
            String querySql = dmlBuilder
                    .schema(extractProperties.getSchema())
                    .columns(metadata.getColumnsMetas())
                    .tableName(tableName)
                    .conditionCompositePrimary(primaryMetas)
                    .build();
            List<Object[]> batchParam = dmlBuilder.conditionCompositePrimaryValue(primaryMetas, compositeKeys);
            return queryColumnValuesByCompositePrimary(querySql, batchParam);
        }
    }

    /**
     * 复合主键表数据查询
     *
     * @param selectDml  查询SQL
     * @param batchParam 复合主键查询参数
     * @return 查询数据结果
     */
    private List<Map<String, String>> queryColumnValuesByCompositePrimary(String selectDml, List<Object[]> batchParam) {
        // 查询当前任务数据，并对数据进行规整
        HashMap<String, Object> paramMap = new HashMap<>();
        paramMap.put(DmlBuilder.PRIMARY_KEYS, batchParam);

        return queryColumnValues(selectDml, paramMap);
    }

    /**
     * 单一主键表数据查询
     *
     * @param selectDml   查询SQL
     * @param primaryKeys 查询主键集合
     * @return 查询数据结果
     */
    private List<Map<String, String>> queryColumnValues(String selectDml, List<String> primaryKeys) {
        // 查询当前任务数据，并对数据进行规整
        HashMap<String, Object> paramMap = new HashMap<>();
        paramMap.put(DmlBuilder.PRIMARY_KEYS, primaryKeys);

        return queryColumnValues(selectDml, paramMap);
    }

    /**
     * 主键表数据查询
     *
     * @param selectDml 查询SQL
     * @param paramMap  查询参数
     * @return 查询结果
     */
    private List<Map<String, String>> queryColumnValues(String selectDml, Map<String, Object> paramMap) {
        // 使用JDBC查询当前任务抽取数据
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(jdbcTemplateOne);
        return jdbc.query(selectDml, paramMap, (rs, rowNum) -> {
            // 获取当前结果集对应的元数据信息
            ResultSetMetaData metaData = rs.getMetaData();
            // 结果集处理器
            ResultSetHandler handler = new ResultSetHandler();
            // 查询结果集 根据元数据信息 进行数据转换
            return handler.putOneResultSetToMap(rs, metaData);
        });
    }

    /**
     * 构建指定表的 Replace SQL语句
     *
     * @param tableName       表名称
     * @param compositeKeySet 复合主键集合
     * @param metadata        元数据信息
     * @return 返回SQL列表
     */
    public List<String> buildReplace(String schema, String tableName, Set<String> compositeKeySet, TableMetadata metadata) {
        List<String> resultList = new ArrayList<>();
        final String localSchema = getLocalSchema(schema);
        ReplaceDmlBuilder builder = new ReplaceDmlBuilder().schema(localSchema)
                .tableName(tableName)
                .columns(metadata.getColumnsMetas());

        List<Map<String, String>> columnValues = queryColumnValues(tableName, new ArrayList<>(compositeKeySet), metadata);
        Map<String, Map<String, String>> compositeKeyValues = transtlateColumnValues(columnValues, metadata.getPrimaryMetas());
        compositeKeySet.forEach(compositeKey -> {
            Map<String, String> columnValue = compositeKeyValues.get(compositeKey);
            if (Objects.nonNull(columnValue) && !columnValue.isEmpty()) {
                resultList.add(builder.columnsValue(columnValue, metadata.getColumnsMetas()).build());
            }
        });
        return resultList;
    }


    /**
     * 构建指定表的 Insert SQL语句
     *
     * @param tableName       表名称
     * @param compositeKeySet 复合主键集合
     * @param metadata        元数据信息
     * @return 返回SQL列表
     */
    public List<String> buildInsert(String schema, String tableName, Set<String> compositeKeySet, TableMetadata metadata) {

        List<String> resultList = new ArrayList<>();
        final String localSchema = getLocalSchema(schema);
        InsertDmlBuilder builder = new InsertDmlBuilder().schema(localSchema)
                .tableName(tableName)
                .columns(metadata.getColumnsMetas());

        List<Map<String, String>> columnValues = queryColumnValues(tableName, new ArrayList<>(compositeKeySet), metadata);
        Map<String, Map<String, String>> compositeKeyValues = transtlateColumnValues(columnValues, metadata.getPrimaryMetas());
        compositeKeySet.forEach(compositeKey -> {
            Map<String, String> columnValue = compositeKeyValues.get(compositeKey);
            if (Objects.nonNull(columnValue) && !columnValue.isEmpty()) {
                resultList.add(builder.columnsValue(columnValue, metadata.getColumnsMetas()).build());
            }
        });
        return resultList;
    }

    private Map<String, Map<String, String>> transtlateColumnValues(List<Map<String, String>> columnValues, List<ColumnsMetaData> primaryMetas) {
        final List<String> primaryKeys = getCompositeKeyColumns(primaryMetas);
        Map<String, Map<String, String>> map = new HashMap<>();
        columnValues.forEach(values -> {
            map.put(getCompositeKey(values, primaryKeys), values);
        });
        return map;
    }

    private List<String> getCompositeKeyColumns(List<ColumnsMetaData> primaryMetas) {
        return primaryMetas.stream().map(ColumnsMetaData::getColumnName).collect(Collectors.toUnmodifiableList());
    }

    private String getCompositeKey(Map<String, String> columnValues, List<String> primaryKeys) {
        return primaryKeys.stream().map(key -> columnValues.get(key)).collect(Collectors.joining(ExtConstants.PRIMARY_DELIMITER));
    }


    /**
     * 构建指定表的批量 Delete SQL语句
     *
     * @param tableName       表名称
     * @param compositeKeySet 复合主键集合
     * @param primaryMetas    主键元数据信息
     * @return 返回SQL列表
     */
    public List<String> buildBatchDelete(String schema, String tableName, Set<String> compositeKeySet, List<ColumnsMetaData> primaryMetas) {
        List<String> resultList = new ArrayList<>();
        final String localSchema = getLocalSchema(schema);
        if (primaryMetas.size() == 1) {
            final ColumnsMetaData primaryMeta = primaryMetas.stream().findFirst().get();
            compositeKeySet.forEach(compositeKey -> {
                final String deleteDml = new BatchDeleteDmlBuilder()
                        .tableName(tableName)
                        .schema(localSchema)
                        .conditionPrimary(primaryMeta)
                        .build();
                resultList.add(deleteDml);
            });
        } else {
            compositeKeySet.forEach(compositeKey -> {
                resultList.add(new BatchDeleteDmlBuilder()
                        .tableName(tableName)
                        .schema(localSchema)
                        .conditionCompositePrimary(primaryMetas)
                        .build());
            });
        }

        return resultList;
    }

    /**
     * 构建指定表的 Delete SQL语句
     *
     * @param tableName       表名称
     * @param compositeKeySet 复合主键集合
     * @param primaryMetas    主键元数据信息
     * @return 返回SQL列表
     */
    public List<String> buildDelete(String schema, String tableName, Set<String> compositeKeySet, List<ColumnsMetaData> primaryMetas) {

        List<String> resultList = new ArrayList<>();
        final String localSchema = getLocalSchema(schema);
        if (primaryMetas.size() == 1) {
            final ColumnsMetaData primaryMeta = primaryMetas.stream().findFirst().get();
            compositeKeySet.forEach(compositeKey -> {
                final String deleteDml = new DeleteDmlBuilder()
                        .tableName(tableName)
                        .schema(localSchema)
                        .condition(primaryMeta, compositeKey)
                        .build();
                resultList.add(deleteDml);
            });
        } else {
            compositeKeySet.forEach(compositeKey -> {
                resultList.add(new DeleteDmlBuilder()
                        .tableName(tableName)
                        .schema(localSchema)
                        .conditionCompositePrimary(compositeKey, primaryMetas)
                        .build());
            });
        }

        return resultList;
    }

    private String getLocalSchema(String schema) {
        if (StringUtils.isEmpty(schema)) {
            return extractProperties.getSchema();
        }
        return schema;
    }

    /**
     * 查询当前表结构元数据信息，并进行Hash
     *
     * @param tableName 表名称
     * @return 表结构Hash
     */
    public TableMetadataHash queryTableMetadataHash(String tableName) {
        final TableMetadataHash tableMetadataHash = new TableMetadataHash().setTableName(tableName);
        final List<ColumnsMetaData> columnsMetaData = metaDataService.queryTableColumnMetaDataOfSchema(tableName);
        StringBuffer buffer = new StringBuffer();
        if (!CollectionUtils.isEmpty(columnsMetaData)) {
            columnsMetaData.sort(Comparator.comparing(ColumnsMetaData::getColumnName));
            columnsMetaData.forEach(column -> {
                buffer.append(column.getColumnName())
                        .append(column.getColumnType())
                        .append(column.getDataType())
                        .append(column.getOrdinalPosition());
            });
        }
        tableMetadataHash.setTableHash(HashUtil.hashBytes(buffer.toString()));
        return tableMetadataHash;
    }
}
