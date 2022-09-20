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
import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.common.entry.enums.CheckBlackWhiteMode;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.dao.DataBaseMetaDataDAOImpl;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
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
    private boolean isQueryTableRowCount = true;

    /**
     * Metadata cache load
     */
    @PostConstruct
    public void init() {
        MetaDataCache.removeAll();
        Map<String, TableMetadata> metaDataMap = queryMetaDataOfSchema();
        MetaDataCache.initCache();
        MetaDataCache.putMap(metaDataMap);
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
            log.info("Query database metadata information completed total=" + columnsMetadata.size());
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
        List<ColumnsMetaData> columnsMetadata = dataBaseMetadataDAOImpl.queryColumnMetadata(List.of(tableName));
        tableMetadata.setColumnsMetas(columnsMetadata).setPrimaryMetas(getTablePrimaryColumn(columnsMetadata));
        log.info("Query database metadata information completed total={}", columnsMetadata);
        return tableMetadata;
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
        return tableMetadatas.stream().filter(meta -> StringUtils.equalsIgnoreCase(meta.getTableName(), tableName))
                             .findFirst().orElseGet(null);
    }

    private List<TableMetadata> queryTableMetadata() {
        if (isQueryTableRowCount) {
            return dataBaseMetadataDAOImpl.queryTableMetadata();
        } else {
            return dataBaseMetadataDAOImpl.queryTableMetadataFast();
        }
    }

    private List<ColumnsMetaData> getTablePrimaryColumn(List<ColumnsMetaData> columnsMetaData) {
        return columnsMetaData.stream().filter(meta -> ColumnKey.PRI.equals(meta.getColumnKey()))
                              .collect(Collectors.toList());
    }
}
