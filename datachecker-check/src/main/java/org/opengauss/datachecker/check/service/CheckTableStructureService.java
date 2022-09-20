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

package org.opengauss.datachecker.check.service;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.check.modules.check.AbstractCheckDiffResultBuilder.CheckDiffResultBuilder;
import org.opengauss.datachecker.check.modules.check.CheckDiffResult;
import org.opengauss.datachecker.check.modules.check.ExportCheckResult;
import org.opengauss.datachecker.check.modules.task.TaskManagerService;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * CheckTableStructureService
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/8
 * @since ：11
 */
@Slf4j
@Service
public class CheckTableStructureService {
    @Autowired
    private TaskManagerService taskManagerService;
    @Autowired
    private EndpointMetaDataManager endpointMetaDataManager;
    @Value("${data.check.data-path}")
    private String checkResultPath;
    private final CompareTableStructure tableStructureCompare = (source, sink) -> {
        if (source.size() == sink.size()) {
            final List<String> sourceUpperList =
                source.stream().map(ColumnsMetaData::getColumnName).map(String::toUpperCase)
                      .collect(Collectors.toList());
            final List<String> diffKeyList = sink.stream().map(ColumnsMetaData::getColumnName).map(String::toUpperCase)
                                                 .filter(key -> !sourceUpperList.contains(key))
                                                 .collect(Collectors.toList());
            return diffKeyList.isEmpty();
        } else {
            return false;
        }
    };

    /**
     * Table structure definition field name verification
     */
    public void check() {
        checkMissTable();
        checkTableStructureChanged();
    }

    private void checkTableStructureChanged() {
        final List<String> checkTableList = endpointMetaDataManager.getCheckTableList();
        taskManagerService.initTableExtractStatus(checkTableList);
        checkTableList.forEach(tableName -> {
            final TableMetadata sourceMeta = endpointMetaDataManager.getTableMetadata(Endpoint.SOURCE, tableName);
            final TableMetadata sinkMeta = endpointMetaDataManager.getTableMetadata(Endpoint.SINK, tableName);
            checkTableStructureChanged(tableName, sourceMeta, sinkMeta);
        });
    }

    private void checkMissTable() {
        final List<String> missTableList = endpointMetaDataManager.getMissTableList();
        missTableList.forEach(missTable -> {
            final TableMetadata sourceMeta = endpointMetaDataManager.getTableMetadata(Endpoint.SOURCE, missTable);
            checkMissTable(missTable, sourceMeta);
        });
    }

    private void checkTableStructureChanged(String tableName, TableMetadata sourceMeta, TableMetadata sinkMeta) {
        final boolean isTableStructureEquals = isTableStructureEquals(sourceMeta, sinkMeta);
        if (!isTableStructureEquals) {
            taskManagerService.refreshTableExtractStatus(tableName, Endpoint.CHECK, -1);
            CheckDiffResult result =
                CheckDiffResultBuilder.builder(null).table(tableName).isTableStructureEquals(false).build();
            ExportCheckResult.export(checkResultPath, result);
            log.error("compared the field names in table[{}](case ignored) and the result is not match", tableName);
        }
    }

    private void checkMissTable(String tableName, TableMetadata sourceMeta) {
        Endpoint onlyExistEndpoint = Objects.isNull(sourceMeta) ? Endpoint.SINK : Endpoint.SOURCE;
        CheckDiffResult result =
            CheckDiffResultBuilder.builder(null).table(tableName).isExistTableMiss(true, onlyExistEndpoint).build();
        ExportCheckResult.export(checkResultPath, result);
        log.error("compared the field names in table[{}](case ignored) and the result is not match", tableName);
    }

    private boolean isTableNotExist(TableMetadata sourceMeta, TableMetadata sinkMeta) {
        // one or double endpoint table have not exists, then return false
        return Objects.isNull(sourceMeta) || Objects.isNull(sinkMeta);
    }

    private boolean isTableStructureEquals(TableMetadata sourceMeta, TableMetadata sinkMeta) {
        // one or double endpoint table have not exists, then return false
        if (isTableNotExist(sourceMeta, sinkMeta)) {
            return false;
        }
        return tableStructureCompare.compare(sourceMeta.getPrimaryMetas(), sinkMeta.getPrimaryMetas())
            && tableStructureCompare.compare(sourceMeta.getColumnsMetas(), sinkMeta.getColumnsMetas());
    }

    @FunctionalInterface
    interface CompareTableStructure {
        /**
         * Compare whether the source and destination table structures are the same
         *
         * @param source source
         * @param sink   sink
         * @return Compare Results
         */
        boolean compare(List<ColumnsMetaData> source, List<ColumnsMetaData> sink);
    }
}
