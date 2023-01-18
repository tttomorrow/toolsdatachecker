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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.opengauss.datachecker.common.constant.Constants;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.enums.DataBaseMeta;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.MetadataLoadProcess;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.EnumUtil;
import org.opengauss.datachecker.common.util.SqlUtil;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.service.RuleAdapterService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCountCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.opengauss.datachecker.extract.constants.ExtConstants.COLUMN_INDEX_FIRST_ZERO;

/**
 * DataBaseMetaDataDAOImpl
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/23
 * @since ：11
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DataBaseMetaDataDAOImpl implements MetaDataDAO {
    private static final String OPEN_GAUSS_PARALLEL_QUERY = "set query_dop to %s;";
    protected final JdbcTemplate JdbcTemplateOne;
    private final RuleAdapterService ruleAdapterService;
    private final ExtractProperties extractProperties;
    private volatile MetadataLoadProcess metadataLoadProcess = new MetadataLoadProcess();

    @Override
    public boolean health() {
        String sql = MetaSqlMapper.getMetaSql(extractProperties.getDatabaseType(), DataBaseMeta.HEALTH);
        List<String> result = new ArrayList<>();
        JdbcTemplateOne.query(sql, ps -> ps.setString(1, getSchema()), new RowCountCallbackHandler() {
            @Override
            protected void processRow(ResultSet rs, int rowNum) throws SQLException {
                result.add(rs.getString(1));
            }
        });
        return result.size() > 0;
    }

    @Override
    public List<TableMetadata> queryTableMetadata() {
        final List<String> tableNameList = filterByTableRules(queryAllTableNames());
        if (CollectionUtils.isEmpty(tableNameList)) {
            return new ArrayList<>();
        }
        final List<TableMetadata> tableMetadataList =
            tableNameList.stream().map(tableName -> new TableMetadata().setTableName(tableName).setTableRows(-1))
                         .collect(Collectors.toList());
        matchRowRules(tableMetadataList);
        return tableMetadataList;
    }

    private void matchRowRules(List<TableMetadata> tableMetadataList) {
        if (CollectionUtils.isEmpty(tableMetadataList)) {
            return;
        }
        ruleAdapterService.executeRowRule(tableMetadataList);
    }

    public List<String> queryAllTableNames() {
        final List<String> tableNameList = new ArrayList<>();
        String sql = MetaSqlMapper.getMetaSql(extractProperties.getDatabaseType(), DataBaseMeta.TABLE);
        JdbcTemplateOne.query(sql, ps -> ps.setString(1, getSchema()), new RowCountCallbackHandler() {
            @Override
            protected void processRow(ResultSet rs, int rowNum) throws SQLException {
                tableNameList.add(rs.getString(1));
            }
        });
        return tableNameList;
    }

    private List<String> filterByTableRules(List<String> tableNameList) {
        return ruleAdapterService.executeTableRule(tableNameList);
    }

    public void getAllTableCount(Set<String> tableNameList) {
        final AtomicInteger tableCount = new AtomicInteger(0);
        String sqlQueryTableRowCount = MetaSqlMapper.getTableCount();
        final String schema = getSchema();
        metadataLoadProcess.setTotal(tableNameList.size());
        tableNameList.parallelStream().forEach(tableName -> {
            enableDatabaseParallelQuery();
            final Long rowCount = JdbcTemplateOne
                .queryForObject(String.format(sqlQueryTableRowCount, escape(schema), escape(tableName)), Long.class);
            MetaDataCache.updateRowCount(tableName, rowCount);
            log.debug("load table [{}]row count={}  total={} ", tableName, rowCount, tableCount.incrementAndGet());
            metadataLoadProcess.setLoadCount(tableCount.get());
        });
    }

    private void enableDatabaseParallelQuery() {
        if (Objects.equals(DataBaseType.OG, extractProperties.getDatabaseType())) {
            JdbcTemplateOne.execute(String.format(OPEN_GAUSS_PARALLEL_QUERY, extractProperties.getQueryDop()));
        }
    }

    private String escape(String content) {
        return SqlUtil.escape(content, extractProperties.getDatabaseType());
    }

    @Override
    public MetadataLoadProcess getMetadataLoadProcess() {
        return metadataLoadProcess;
    }

    @Override
    public List<ColumnsMetaData> queryColumnMetadata(String tableName) {
        return queryColumnMetadata(List.of(tableName));
    }

    @Override
    public List<ColumnsMetaData> queryColumnMetadata(List<String> tableNames) {
        Map<String, Object> map = new HashMap<>(Constants.InitialCapacity.EMPTY);
        map.put("tableNames", tableNames);
        map.put("databaseSchema", getSchema());
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(JdbcTemplateOne);
        String sql = MetaSqlMapper.getMetaSql(extractProperties.getDatabaseType(), DataBaseMeta.COLUMN);
        List<ColumnsMetaData> columns = jdbc.query(sql, map, new RowMapper<>() {
            int columnIndex = COLUMN_INDEX_FIRST_ZERO;

            @Override
            public ColumnsMetaData mapRow(ResultSet rs, int rowNum) throws SQLException {
                ColumnsMetaData columnsMetaData = new ColumnsMetaData();
                columnsMetaData.setTableName(rs.getString(++columnIndex)).setColumnName(rs.getString(++columnIndex))
                               .setOrdinalPosition(rs.getInt(++columnIndex)).setDataType(rs.getString(++columnIndex))
                               .setColumnType(rs.getString(++columnIndex))
                               .setColumnKey(EnumUtil.valueOf(ColumnKey.class, rs.getString(++columnIndex)));
                columnIndex = COLUMN_INDEX_FIRST_ZERO;
                return columnsMetaData;
            }
        });
        return ruleAdapterService.executeColumnRule(columns);
    }

    /**
     * Dynamically obtain the schema information of the current data source
     *
     * @return database schema
     */
    private String getSchema() {
        return extractProperties.getSchema();
    }
}
