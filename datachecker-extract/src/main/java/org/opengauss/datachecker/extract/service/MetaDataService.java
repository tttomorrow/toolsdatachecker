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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author wang chao
 * @description 元数据服务
 * @date 2022/5/8 19:27
 * @since 11
 **/
@Service
@Slf4j
@RequiredArgsConstructor
public class MetaDataService {

    private final DataBaseMetaDataDAOImpl dataBaseMetadataDAOImpl;

    @Value("${spring.extract.query-table-row-count}")
    private boolean queryTableRowCount;

    @PostConstruct
    public void init() {
        MetaDataCache.removeAll();
        Map<String, TableMetadata> metaDataMap = queryMetaDataOfSchema();
        MetaDataCache.initCache();
        MetaDataCache.putMap(metaDataMap);
    }

    public Map<String, TableMetadata> queryMetaDataOfSchema() {

        List<TableMetadata> tableMetadata = queryTableMetadata();
        List<String> tableNames = tableMetadata
                .stream()
                .map(TableMetadata::getTableName)
                .collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(tableNames)) {
            List<ColumnsMetaData> columnsMetadata = dataBaseMetadataDAOImpl.queryColumnMetadata(tableNames);
            Map<String, List<ColumnsMetaData>> tableColumnMap = columnsMetadata.stream().collect(Collectors.groupingBy(ColumnsMetaData::getTableName));

            tableMetadata.stream().forEach(tableMeta -> {
                tableMeta.setColumnsMetas(tableColumnMap.get(tableMeta.getTableName()))
                        .setPrimaryMetas(getTablePrimaryColumn(tableColumnMap.get(tableMeta.getTableName())));
            });
            log.info("查询数据库元数据信息完成 total=" + columnsMetadata.size());
        }
        return tableMetadata.stream().collect(Collectors.toMap(TableMetadata::getTableName, Function.identity()));
    }

    public void refushBlackWhiteList(CheckBlackWhiteMode mode, List<String> tableList) {
        dataBaseMetadataDAOImpl.resetBlackWhite(mode, tableList);
        init();
    }

    public TableMetadata queryMetaDataOfSchema(String tableName) {
        TableMetadata tableMetadata = queryTableMetadataByTableName(tableName);
        List<ColumnsMetaData> columnsMetadata = dataBaseMetadataDAOImpl.queryColumnMetadata(List.of(tableName));
        tableMetadata
                .setColumnsMetas(columnsMetadata)
                .setPrimaryMetas(getTablePrimaryColumn(columnsMetadata));

        log.info("查询数据库元数据信息完成 total={}", columnsMetadata);
        return tableMetadata;
    }

    public List<ColumnsMetaData> queryTableColumnMetaDataOfSchema(String tableName) {
        return dataBaseMetadataDAOImpl.queryColumnMetadata(List.of(tableName));
    }

    private TableMetadata queryTableMetadataByTableName(String tableName) {
        final List<TableMetadata> tableMetadatas = queryTableMetadata();
        return tableMetadatas.stream()
                .filter(meta -> StringUtils.equalsIgnoreCase(meta.getTableName(), tableName))
                .findFirst()
                .orElseGet(null);
    }

    private List<TableMetadata> queryTableMetadata() {
        if (queryTableRowCount) {
            return dataBaseMetadataDAOImpl.queryTableMetadata();
        } else {
            return dataBaseMetadataDAOImpl.queryTableMetadataFast();
        }
    }

    /**
     * 获取表主键列元数据信息
     *
     * @param columnsMetaData
     * @return
     */
    private List<ColumnsMetaData> getTablePrimaryColumn(List<ColumnsMetaData> columnsMetaData) {
        return columnsMetaData.stream()
                .filter(meta -> ColumnKey.PRI.equals(meta.getColumnKey()))
                .collect(Collectors.toList());
    }

}
