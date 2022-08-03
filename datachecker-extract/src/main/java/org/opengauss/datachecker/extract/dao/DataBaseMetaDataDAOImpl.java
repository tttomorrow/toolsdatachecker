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
import org.opengauss.datachecker.common.constant.Constants;
import org.opengauss.datachecker.common.entry.enums.CheckBlackWhiteMode;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.enums.DataBaseMeta;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.EnumUtil;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCountCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
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

    private static final AtomicReference<CheckBlackWhiteMode> MODE_REF =
        new AtomicReference<>(CheckBlackWhiteMode.NONE);
    private static final AtomicReference<List<String>> WHITE_REF = new AtomicReference<>();
    private static final AtomicReference<List<String>> BLACK_REF = new AtomicReference<>();

    protected final JdbcTemplate JdbcTemplateOne;

    private final ExtractProperties extractProperties;

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
    public void resetBlackWhite(CheckBlackWhiteMode mode, List<String> tableList) {
        MODE_REF.set(mode);
        if (Objects.equals(mode, CheckBlackWhiteMode.WHITE)) {
            WHITE_REF.set(tableList);
        } else if (Objects.equals(mode, CheckBlackWhiteMode.BLACK)) {
            BLACK_REF.set(tableList);
        } else {
            WHITE_REF.getAcquire().clear();
            BLACK_REF.getAcquire().clear();
        }
    }

    @Override
    public List<TableMetadata> queryTableMetadata() {
        final List<String> tableNameList = new ArrayList<>();
        String sql = MetaSqlMapper.getMetaSql(extractProperties.getDatabaseType(), DataBaseMeta.TABLE);
        JdbcTemplateOne.query(sql, ps -> ps.setString(1, getSchema()), new RowCountCallbackHandler() {
            @Override
            protected void processRow(ResultSet rs, int rowNum) throws SQLException {
                tableNameList.add(rs.getString(1));
            }
        });
        return getAllTableCount(filterTableListByBlackWhite(tableNameList));
    }

    private List<String> filterTableListByBlackWhite(List<String> tableNameList) {
        if (Objects.equals(MODE_REF.get(), CheckBlackWhiteMode.WHITE)) {
            final List<String> whiteList = WHITE_REF.get();
            if (CollectionUtils.isEmpty(whiteList)) {
                return tableNameList;
            }
            return tableNameList.stream().filter(whiteList::contains).collect(Collectors.toList());
        } else if (Objects.equals(MODE_REF.get(), CheckBlackWhiteMode.BLACK)) {
            final List<String> blackList = BLACK_REF.get();
            if (CollectionUtils.isEmpty(blackList)) {
                return tableNameList;
            }
            return tableNameList.stream().filter(table -> !blackList.contains(table)).collect(Collectors.toList());
        } else {
            return tableNameList;
        }
    }

    private List<TableMetadata> filterBlackWhiteList(List<TableMetadata> tableMetaList) {
        if (Objects.equals(MODE_REF.get(), CheckBlackWhiteMode.WHITE)) {
            final List<String> whiteList = WHITE_REF.get();
            if (CollectionUtils.isEmpty(whiteList)) {
                return tableMetaList;
            }
            return tableMetaList.stream().filter(table -> whiteList.contains(table.getTableName()))
                                .collect(Collectors.toList());
        } else if (Objects.equals(MODE_REF.get(), CheckBlackWhiteMode.BLACK)) {
            final List<String> blackList = BLACK_REF.get();
            if (CollectionUtils.isEmpty(blackList)) {
                return tableMetaList;
            }
            return tableMetaList.stream().filter(table -> !blackList.contains(table.getTableName()))
                                .collect(Collectors.toList());
        } else {
            return tableMetaList;
        }
    }

    @Override
    public List<TableMetadata> queryTableMetadataFast() {
        List<TableMetadata> tableMetadata = new ArrayList<>();
        String sql = MetaSqlMapper.getMetaSql(extractProperties.getDatabaseType(), DataBaseMeta.TABLE);
        JdbcTemplateOne.query(sql, ps -> ps.setString(1, getSchema()), new RowCountCallbackHandler() {
            @Override
            protected void processRow(ResultSet rs, int rowNum) throws SQLException {
                final TableMetadata metadata =
                    new TableMetadata().setTableName(rs.getString(1)).setTableRows(rs.getLong(2));
                log.debug("queryTableMetadataFast {}", metadata.toString());
                tableMetadata.add(metadata);
            }
        });
        return filterBlackWhiteList(tableMetadata);
    }

    private List<TableMetadata> getAllTableCount(List<String> tableNameList) {
        final List<TableMetadata> tableMetadata = new ArrayList<>();
        String sqlQueryTableRowCount = MetaSqlMapper.getTableCount();
        final String schema = getSchema();
        tableNameList.stream().forEach(tableName -> {
            final Long rowCount =
                JdbcTemplateOne.queryForObject(String.format(sqlQueryTableRowCount, schema, tableName), Long.class);
            tableMetadata.add(new TableMetadata().setTableName(tableName).setTableRows(rowCount));
        });
        return tableMetadata;
    }

    @Override
    public List<ColumnsMetaData> queryColumnMetadata(String tableName) {
        return queryColumnMetadata(Arrays.asList(tableName));
    }

    @Override
    public List<ColumnsMetaData> queryColumnMetadata(List<String> tableNames) {
        Map<String, Object> map = new HashMap<>(Constants.InitialCapacity.MAP);
        map.put("tableNames", tableNames);
        map.put("databaseSchema", getSchema());
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(JdbcTemplateOne);
        String sql = MetaSqlMapper.getMetaSql(extractProperties.getDatabaseType(), DataBaseMeta.COLUMN);
        return jdbc.query(sql, map, new RowMapper<ColumnsMetaData>() {
            int columnIndex = COLUMN_INDEX_FIRST_ZERO;

            @Override
            public ColumnsMetaData mapRow(ResultSet rs, int rowNum) throws SQLException {

                ColumnsMetaData columnsMetaData = new ColumnsMetaData().setTableName(rs.getString(++columnIndex))
                                                                       .setColumnName(rs.getString(++columnIndex))
                                                                       .setOrdinalPosition(rs.getInt(++columnIndex))
                                                                       .setDataType(rs.getString(++columnIndex))
                                                                       .setColumnType(rs.getString(++columnIndex))
                                                                       .setColumnKey(EnumUtil.valueOf(ColumnKey.class,
                                                                           rs.getString(++columnIndex)));
                columnIndex = COLUMN_INDEX_FIRST_ZERO;
                return columnsMetaData;
            }
        });
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
