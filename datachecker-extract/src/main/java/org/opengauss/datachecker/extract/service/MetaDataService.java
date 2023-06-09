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
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.MetadataLoadProcess;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.thread.ThreadPoolFactory;
import org.opengauss.datachecker.common.util.PhaserUtil;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.dao.DataBaseMetaDataDAOImpl;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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
     * Return database metadata information through cache
     *
     * @return metadata information
     */
    public Map<String, TableMetadata> queryMetaDataOfSchemaCache() {
        return MetaDataCache.getAll();
    }

    public List<String> queryAllTableNames() {
        return dataBaseMetadataDAOImpl.queryTableNameList();
    }

    /**
     * Asynchronous loading of metadata cache information
     */
    public void loadMetaDataOfSchemaCache() {
        if (MetaDataCache.isEmpty()) {
            Map<String, TableMetadata> metaDataMap = queryMetaDataOfSchema();
            MetaDataCache.putMap(metaDataMap);
            log.info("put table metadata in cache [{}]", metaDataMap.size());
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

    public Map<String, TableMetadata> queryMetaDataOfSchema() {
        log.info("query table metadata");
        Map<String, TableMetadata> tableMetadataMap = new ConcurrentHashMap<>();
        final List<TableMetadata> tableMetadataList = dataBaseMetadataDAOImpl.queryTableMetadataList();
        if (CollectionUtils.isEmpty(tableMetadataList)) {
            return tableMetadataMap;
        }
        ExecutorService threadPool =
            ThreadPoolFactory.newThreadPool("batch-query-table-metadata", 4, Integer.MAX_VALUE);
        List<Future<?>> futureList = new LinkedList<>();
        tableMetadataList.forEach(tableMetadata -> {
            futureList.add(threadPool.submit(() -> {
                setTableMetadataByTableName(tableMetadata);
                tableMetadataMap.put(tableMetadata.getTableName(), tableMetadata);
            }));
        });
        PhaserUtil.executorComplete(threadPool, futureList);
        Map<String, TableMetadata> filterNoPrimary = tableMetadataMap.entrySet().stream().filter(
            entry -> CollectionUtils.isNotEmpty(entry.getValue().getPrimaryMetas())).collect(
            Collectors.toMap(Entry::getKey, Entry::getValue));
        tableMetadataMap.clear();
        tableMetadataMap.putAll(filterNoPrimary);
        dataBaseMetadataDAOImpl.matchRowRules(tableMetadataMap);
        log.info("build table metadata [{}]", tableMetadataMap.size());
        return tableMetadataMap;
    }

    private void setTableMetadataByTableName(TableMetadata tableMetadata) {
        List<ColumnsMetaData> columnsMetadatas =
            dataBaseMetadataDAOImpl.queryTableColumnsMetaData(tableMetadata.getTableName());
        tableMetadata.setColumnsMetas(columnsMetadatas);
        tableMetadata.setPrimaryMetas(getTablePrimaryColumn(columnsMetadatas));
    }

    public TableMetadata getMetaDataOfSchemaByCache(String tableName) {
        if (!MetaDataCache.containsKey(tableName)) {
            final TableMetadata tableMetadata = dataBaseMetadataDAOImpl.queryTableMetadata(tableName);
            if (Objects.nonNull(tableMetadata)) {
                MetaDataCache.put(tableName, tableMetadata);
            }
        }
        return MetaDataCache.get(tableName);
    }

    /**
     * query column Metadata info
     *
     * @param tableName tableName
     * @return column Metadata info
     */
    public List<ColumnsMetaData> queryTableColumnMetaDataOfSchema(String tableName) {
        return dataBaseMetadataDAOImpl.queryTableColumnsMetaData(tableName);
    }

    private List<ColumnsMetaData> getTablePrimaryColumn(List<ColumnsMetaData> columnsMetaData) {
        return columnsMetaData.stream().filter(meta -> ColumnKey.PRI.equals(meta.getColumnKey()))
                              .sorted(Comparator.comparing(ColumnsMetaData::getOrdinalPosition))
                              .collect(Collectors.toList());
    }
}
