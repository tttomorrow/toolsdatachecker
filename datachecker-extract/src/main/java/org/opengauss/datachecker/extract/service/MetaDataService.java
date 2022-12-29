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

package org.opengauss.datachecker.extract.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.common.entry.enums.CheckBlackWhiteMode;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.MetadataLoadProcess;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.dao.DataBaseMetaDataDAOImpl;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * MetaDataService
 *
 * @author wang chao
 * @date 2022/5/8 19:27
 * @since 11
 **/
@Service
@Slf4j
@RequiredArgsConstructor
public class MetaDataService {
    private final DataBaseMetaDataDAOImpl dataBaseMetadataDAOImpl;

    /**
     * Metadata cache load
     */
    public void init() {

    }

    /**
     * Return database metadata information through cache
     *
     * @return metadata information
     */
    public Map<String, TableMetadata> queryMetaDataOfSchemaCache() {
        return MetaDataCache.getAll();
    }

    public List<String> queryAllTableNames() {
        return dataBaseMetadataDAOImpl.queryAllTableNames();
    }

    /**
     * Asynchronous loading of metadata cache information
     */
    public void loadMetaDataOfSchemaCache() {
        if (MetaDataCache.isEmpty()) {
            Map<String, TableMetadata> metaDataMap = queryMetaDataOfSchema();
            MetaDataCache.putMap(metaDataMap);
            final Set<String> allTables = MetaDataCache.getAllKeys();
            if (CollectionUtils.isNotEmpty(allTables)) {
                ThreadUtil.newSingleThreadExecutor().submit(() -> {
                    dataBaseMetadataDAOImpl.getAllTableCount(allTables);
                });
            }
            log.debug("load meta data cache");
        }
    }

    /**
     * Return metadata loading progress
     *
     * @return metadata loading progress
     */
    public MetadataLoadProcess getMetadataLoadProcess() {
        return dataBaseMetadataDAOImpl.getMetadataLoadProcess();
    }

    /**
     * query Metadata info
     *
     * @return Metadata info
     */
    public Map<String, TableMetadata> queryMetaDataOfSchema() {
        List<TableMetadata> tableMetadata = queryTableMetadata();
        List<String> tableNames = tableMetadata.stream().map(TableMetadata::getTableName).collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(tableNames)) {
            List<ColumnsMetaData> columnsMetadata = dataBaseMetadataDAOImpl.queryColumnMetadata(tableNames);
            Map<String, List<ColumnsMetaData>> tableColumnMap =
                columnsMetadata.stream().collect(Collectors.groupingBy(ColumnsMetaData::getTableName));
            tableMetadata.forEach(tableMeta -> {
                tableMeta.setColumnsMetas(tableColumnMap.get(tableMeta.getTableName()))
                         .setPrimaryMetas(getTablePrimaryColumn(tableColumnMap.get(tableMeta.getTableName())));
            });
        }
        return tableMetadata.stream().collect(Collectors.toMap(TableMetadata::getTableName, Function.identity()));
    }

    /**
     * refresh black or white list
     *
     * @param mode      mode{@value CheckBlackWhiteMode#API_DESCRIPTION }
     * @param tableList tableList
     */
    public void refreshBlackWhiteList(CheckBlackWhiteMode mode, List<String> tableList) {
        dataBaseMetadataDAOImpl.resetBlackWhite(mode, tableList);
        init();
        log.info("refresh black or white list ,mode=[{}],list=[{}]", mode.getDescription(), tableList);
    }

    /**
     * query table Metadata info
     *
     * @param tableName tableName
     * @return table Metadata info
     */
    public TableMetadata queryMetaDataOfSchema(String tableName) {
        TableMetadata tableMetadata = queryTableMetadataByTableName(tableName);
        if (Objects.isNull(tableMetadata)) {
            return tableMetadata;
        }
        List<ColumnsMetaData> columnsMetadata = dataBaseMetadataDAOImpl.queryColumnMetadata(List.of(tableName));
        tableMetadata.setColumnsMetas(columnsMetadata).setPrimaryMetas(getTablePrimaryColumn(columnsMetadata));
        log.debug("Query database metadata information completed total={}", columnsMetadata);
        return tableMetadata;
    }

    public TableMetadata getMetaDataOfSchemaByCache(String tableName) {
        if (!MetaDataCache.containsKey(tableName)) {
            MetaDataCache.put(tableName, queryMetaDataOfSchema(tableName));
        }
        return MetaDataCache.get(tableName);
    }

    public void updateMetaDataOfSchemaByCache(String tableName) {
        MetaDataCache.put(tableName, queryMetaDataOfSchema(tableName));
    }

    /**
     * query column Metadata info
     *
     * @param tableName tableName
     * @return column Metadata info
     */
    public List<ColumnsMetaData> queryTableColumnMetaDataOfSchema(String tableName) {
        return dataBaseMetadataDAOImpl.queryColumnMetadata(List.of(tableName));
    }

    private TableMetadata queryTableMetadataByTableName(String tableName) {
        final List<TableMetadata> tableMetadatas = queryTableMetadata();
        return tableMetadatas.stream().filter(meta -> StringUtils.equals(meta.getTableName(), tableName)).findFirst()
                             .orElse(null);
    }

    private List<TableMetadata> queryTableMetadata() {
        return dataBaseMetadataDAOImpl.queryTableMetadata();
    }

    private List<ColumnsMetaData> getTablePrimaryColumn(List<ColumnsMetaData> columnsMetaData) {
        return columnsMetaData.stream().filter(meta -> ColumnKey.PRI.equals(meta.getColumnKey()))
                              .collect(Collectors.toList());
    }
}
