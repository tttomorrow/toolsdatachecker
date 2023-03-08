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
import org.apache.commons.collections4.MapUtils;
import org.opengauss.datachecker.common.constant.Constants;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.enums.DataBaseMeta;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.MetadataLoadProcess;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.EnumUtil;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.service.RuleAdapterService;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowCountCallbackHandler;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    public static final String TABLE_NAMES = "tableNames";
    public static final String DATABASE_SCHEMA = "databaseSchema";

    protected final JdbcTemplate JdbcTemplateOne;
    private final RuleAdapterService ruleAdapterService;
    private final ExtractProperties extractProperties;
    private volatile MetadataLoadProcess metadataLoadProcess = new MetadataLoadProcess();

    @Override
    public boolean health() {
        String sql = MetaSqlMapper.getMetaSql(extractProperties.getDatabaseType(), DataBaseMeta.HEALTH);
        List<String> result = new ArrayList<>();
        JdbcTemplateOne
            .query(sql, (PreparedStatementSetter) ps -> ps.setString(1, getSchema()), new RowCountCallbackHandler() {
                @Override
                protected void processRow(ResultSet rs, int rowNum) throws SQLException {
                    result.add(rs.getString(1));
                }
            });
        return result.size() > 0;
    }

    @Override
    public List<String> queryTableNameList() {
        return filterByTableRules(queryAllTableNames());
    }

    @Override
    public void matchRowRules(Map<String, TableMetadata> tableMetadataMap) {
        if (MapUtils.isEmpty(tableMetadataMap)) {
            return;
        }
        ruleAdapterService.executeRowRule(tableMetadataMap);
    }

    private List<String> queryAllTableNames() {
        Map<String, Object> map = new HashMap<>(Constants.InitialCapacity.EMPTY);
        final String schema = getSchema();
        map.put(DATABASE_SCHEMA, schema);
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(JdbcTemplateOne);
        String sql = MetaSqlMapper.getMetaSql(extractProperties.getDatabaseType(), DataBaseMeta.TABLE);
        log.info("query schema [{}] tables", schema);
        log.info("query schema [{}] tables sql : {}", schema, sql);
        LocalDateTime start = LocalDateTime.now();
        final List<String> tableList = jdbc.query(sql, map, (rs, rowNum) -> rs.getString(1));
        log.info("query schema [{}] tables count [{}] cost={}", schema, tableList.size(),
            Duration.between(start, LocalDateTime.now()).toSeconds());
        return tableList;
    }

    private List<String> filterByTableRules(List<String> tableNameList) {
        return ruleAdapterService.executeTableRule(tableNameList);
    }

    @Override
    public MetadataLoadProcess getMetadataLoadProcess() {
        return metadataLoadProcess;
    }

    @Override
    public List<ColumnsMetaData> queryTableColumnsMetaData(String tableName) {
        final String schema = getSchema();
        String sql = MetaSqlMapper.getMetaSql(extractProperties.getDatabaseType(), DataBaseMeta.COLUMN);
        LocalDateTime start = LocalDateTime.now();
        Map<String, Object> map = new HashMap<>(Constants.InitialCapacity.EMPTY);
        map.put(TABLE_NAMES, tableName);
        map.put(DATABASE_SCHEMA, schema);
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(JdbcTemplateOne);
        List<ColumnsMetaData> columns = new LinkedList<>();
        try (Stream<ColumnsMetaData> resultStream = jdbc.queryForStream(sql, map, (rs, rowNum) -> {
            ColumnsMetaData columnsMetaData = new ColumnsMetaData();
            columnsMetaData.setTableName(rs.getString(1)).setColumnName(rs.getString(2))
                           .setOrdinalPosition(rs.getInt(3)).setDataType(rs.getString(4)).setColumnType(rs.getString(5))
                           .setColumnKey(EnumUtil.valueOf(ColumnKey.class, rs.getString(6)));
            return columnsMetaData;
        })) {
            columns.addAll(resultStream.collect(Collectors.toList()));
            log.debug(" query [{}] table columns metadata  column={} cost {}", tableName, columns.size(),
                Duration.between(start, LocalDateTime.now()).toSeconds());
        } catch (DataAccessException exception) {
            log.error("jdbc query sub column metadata [{}] error :", sql, exception);
        }
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