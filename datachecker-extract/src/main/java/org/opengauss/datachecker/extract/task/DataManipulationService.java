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

import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.TableMetadataHash;
import org.opengauss.datachecker.common.util.LongHashFunctionWrapper;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.constants.ExtConstants;
import org.opengauss.datachecker.extract.dml.BatchDeleteDmlBuilder;
import org.opengauss.datachecker.extract.dml.DeleteDmlBuilder;
import org.opengauss.datachecker.extract.dml.DmlBuilder;
import org.opengauss.datachecker.extract.dml.InsertDmlBuilder;
import org.opengauss.datachecker.extract.dml.ReplaceDmlBuilder;
import org.opengauss.datachecker.extract.dml.SelectDmlBuilder;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DML  Data operation service realizes dynamic query of data
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/13
 * @since ：11
 */
@Service
public class DataManipulationService {
    private static final LongHashFunctionWrapper HASH_UTIL = new LongHashFunctionWrapper();

    @Autowired
    private JdbcTemplate jdbcTemplateOne;
    @Autowired
    private MetaDataService metaDataService;
    @Autowired
    private ExtractProperties extractProperties;

    /**
     * queryColumnValues
     *
     * @param tableName     tableName
     * @param compositeKeys compositeKeys
     * @param metadata      metadata
     * @return query result
     */
    public List<Map<String, String>> queryColumnValues(String tableName, List<String> compositeKeys,
        TableMetadata metadata) {
        Assert.isTrue(Objects.nonNull(metadata), "Abnormal table metadata information, failed to build select SQL");
        final List<ColumnsMetaData> primaryMetas = metadata.getPrimaryMetas();

        Assert.isTrue(!CollectionUtils.isEmpty(primaryMetas),
            "The metadata information of the table primary key is abnormal, and the construction of select SQL failed");

        // Single primary key table data query
        if (primaryMetas.size() == 1) {
            final ColumnsMetaData primaryData = primaryMetas.get(0);
            String querySql =
                new SelectDmlBuilder().schema(extractProperties.getSchema()).columns(metadata.getColumnsMetas())
                                      .tableName(tableName).conditionPrimary(primaryData).build();
            return queryColumnValues(querySql, compositeKeys);
        } else {
            // Compound primary key table data query
            final SelectDmlBuilder dmlBuilder = new SelectDmlBuilder();
            String querySql = dmlBuilder.schema(extractProperties.getSchema()).columns(metadata.getColumnsMetas())
                                        .tableName(tableName).conditionCompositePrimary(primaryMetas).build();
            List<Object[]> batchParam = dmlBuilder.conditionCompositePrimaryValue(primaryMetas, compositeKeys);
            return queryColumnValuesByCompositePrimary(querySql, batchParam);
        }
    }

    /**
     * Compound primary key table data query
     *
     * @param selectDml  Query SQL
     * @param batchParam Compound PK query parameters
     * @return Query data results
     */
    private List<Map<String, String>> queryColumnValuesByCompositePrimary(String selectDml, List<Object[]> batchParam) {
        // Query the current task data and organize the data
        HashMap<String, Object> paramMap = new HashMap<>();
        paramMap.put(DmlBuilder.PRIMARY_KEYS, batchParam);
        return queryColumnValues(selectDml, paramMap);
    }

    /**
     * Single primary key table data query
     *
     * @param selectDml   Query SQL
     * @param primaryKeys Query primary key collection
     * @return Query data results
     */
    private List<Map<String, String>> queryColumnValues(String selectDml, List<String> primaryKeys) {
        // Query the current task data and organize the data
        HashMap<String, Object> paramMap = new HashMap<>();
        paramMap.put(DmlBuilder.PRIMARY_KEYS, primaryKeys);
        return queryColumnValues(selectDml, paramMap);
    }

    /**
     * Primary key table data query
     *
     * @param selectDml Query SQL
     * @param paramMap  query parameters
     * @return query result
     */
    private List<Map<String, String>> queryColumnValues(String selectDml, Map<String, Object> paramMap) {
        // Use JDBC to query the current task to extract data
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(jdbcTemplateOne);
        return jdbc.query(selectDml, paramMap, (rs, rowNum) -> {
            // Get the metadata information corresponding to the current result set
            ResultSetMetaData metaData = rs.getMetaData();
            // Result set processor
            ResultSetHandler handler = new ResultSetHandler();
            // Data conversion of query result set according to metadata information
            return handler.putOneResultSetToMap(rs, metaData);
        });
    }

    /**
     * Build the replace SQL statement of the specified table
     *
     * @param tableName       tableName
     * @param compositeKeySet composite key set
     * @param metadata        metadata
     * @return Return to SQL list
     */
    public List<String> buildReplace(String schema, String tableName, Set<String> compositeKeySet,
        TableMetadata metadata) {
        List<String> resultList = new ArrayList<>();
        final String localSchema = getLocalSchema(schema);
        ReplaceDmlBuilder builder =
            new ReplaceDmlBuilder().schema(localSchema).tableName(tableName).columns(metadata.getColumnsMetas());

        List<Map<String, String>> columnValues =
            queryColumnValues(tableName, new ArrayList<>(compositeKeySet), metadata);
        Map<String, Map<String, String>> compositeKeyValues =
            transtlateColumnValues(columnValues, metadata.getPrimaryMetas());
        compositeKeySet.forEach(compositeKey -> {
            Map<String, String> columnValue = compositeKeyValues.get(compositeKey);
            if (Objects.nonNull(columnValue) && !columnValue.isEmpty()) {
                resultList.add(builder.columnsValue(columnValue, metadata.getColumnsMetas()).build());
            }
        });
        return resultList;
    }

    /**
     * Build the insert SQL statement of the specified table
     *
     * @param tableName       tableName
     * @param compositeKeySet composite key set
     * @param metadata        metadata
     * @return Return to SQL list
     */
    public List<String> buildInsert(String schema, String tableName, Set<String> compositeKeySet,
        TableMetadata metadata) {

        List<String> resultList = new ArrayList<>();
        final String localSchema = getLocalSchema(schema);
        InsertDmlBuilder builder =
            new InsertDmlBuilder().schema(localSchema).tableName(tableName).columns(metadata.getColumnsMetas());

        List<Map<String, String>> columnValues =
            queryColumnValues(tableName, new ArrayList<>(compositeKeySet), metadata);
        Map<String, Map<String, String>> compositeKeyValues =
            transtlateColumnValues(columnValues, metadata.getPrimaryMetas());
        compositeKeySet.forEach(compositeKey -> {
            Map<String, String> columnValue = compositeKeyValues.get(compositeKey);
            if (Objects.nonNull(columnValue) && !columnValue.isEmpty()) {
                resultList.add(builder.columnsValue(columnValue, metadata.getColumnsMetas()).build());
            }
        });
        return resultList;
    }

    private Map<String, Map<String, String>> transtlateColumnValues(List<Map<String, String>> columnValues,
        List<ColumnsMetaData> primaryMetas) {
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
        return primaryKeys.stream().map(key -> columnValues.get(key))
                          .collect(Collectors.joining(ExtConstants.PRIMARY_DELIMITER));
    }

    /**
     * Build a batch delete SQL statement for the specified table
     *
     * @param tableName       tableName
     * @param compositeKeySet composite key set
     * @param primaryMetas    Primary key metadata information
     * @return Return to SQL list
     */
    public List<String> buildBatchDelete(String schema, String tableName, Set<String> compositeKeySet,
        List<ColumnsMetaData> primaryMetas) {
        List<String> resultList = new ArrayList<>();
        final String localSchema = getLocalSchema(schema);
        if (primaryMetas.size() == 1) {
            final ColumnsMetaData primaryMeta = primaryMetas.stream().findFirst().get();
            compositeKeySet.forEach(compositeKey -> {
                final String deleteDml =
                    new BatchDeleteDmlBuilder().tableName(tableName).schema(localSchema).conditionPrimary(primaryMeta)
                                               .build();
                resultList.add(deleteDml);
            });
        } else {
            compositeKeySet.forEach(compositeKey -> {
                resultList.add(new BatchDeleteDmlBuilder().tableName(tableName).schema(localSchema)
                                                          .conditionCompositePrimary(primaryMetas).build());
            });
        }
        return resultList;
    }

    /**
     * Build the delete SQL statement of the specified table
     *
     * @param tableName       tableName
     * @param compositeKeySet composite key set
     * @param primaryMetas    Primary key metadata information
     * @return Return to SQL list
     */
    public List<String> buildDelete(String schema, String tableName, Set<String> compositeKeySet,
        List<ColumnsMetaData> primaryMetas) {

        List<String> resultList = new ArrayList<>();
        final String localSchema = getLocalSchema(schema);
        if (primaryMetas.size() == 1) {
            final ColumnsMetaData primaryMeta = primaryMetas.stream().findFirst().get();
            compositeKeySet.forEach(compositeKey -> {
                final String deleteDml =
                    new DeleteDmlBuilder().tableName(tableName).schema(localSchema).condition(primaryMeta, compositeKey)
                                          .build();
                resultList.add(deleteDml);
            });
        } else {
            compositeKeySet.forEach(compositeKey -> {
                resultList.add(new DeleteDmlBuilder().tableName(tableName).schema(localSchema)
                                                     .conditionCompositePrimary(compositeKey, primaryMetas).build());
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
     * Query the metadata information of the current table structure and hash
     *
     * @param tableName tableName
     * @return Table structure hash
     */
    public TableMetadataHash queryTableMetadataHash(String tableName) {
        final TableMetadataHash tableMetadataHash = new TableMetadataHash().setTableName(tableName);
        final List<ColumnsMetaData> columnsMetaData = metaDataService.queryTableColumnMetaDataOfSchema(tableName);
        StringBuffer buffer = new StringBuffer();
        if (!CollectionUtils.isEmpty(columnsMetaData)) {
            columnsMetaData.sort(Comparator.comparing(ColumnsMetaData::getColumnName));
            columnsMetaData.forEach(column -> {
                buffer.append(column.getColumnName()).append(column.getColumnType()).append(column.getDataType())
                      .append(column.getOrdinalPosition());
            });
        }
        tableMetadataHash.setTableHash(HASH_UTIL.hashBytes(buffer.toString()));
        return tableMetadataHash;
    }
}
