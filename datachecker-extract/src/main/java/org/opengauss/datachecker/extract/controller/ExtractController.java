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

package org.opengauss.datachecker.extract.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.opengauss.datachecker.common.entry.enums.CheckBlackWhiteMode;
import org.opengauss.datachecker.common.entry.enums.DML;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.TableMetadataHash;
import org.opengauss.datachecker.common.exception.ProcessMultipleException;
import org.opengauss.datachecker.common.exception.TaskNotFoundException;
import org.opengauss.datachecker.common.web.Result;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.service.DataExtractService;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * data extracton service
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/23
 * @since ：11
 */
@Tag(name = "data extracton service")
@RestController
public class ExtractController {

    @Autowired
    private MetaDataService metaDataService;

    @Autowired
    private DataExtractService dataExtractService;

    /**
     * loading database metadata information
     * including the table name,primary key field information list, and column field information list
     *
     * @return database metadata information
     */
    @GetMapping("/extract/load/database/meta/data")
    public Result<Map<String, TableMetadata>> queryMetaDataOfSchema() {
        Map<String, TableMetadata> metaDataMap = metaDataService.queryMetaDataOfSchema();
        MetaDataCache.putMap(metaDataMap);
        return Result.success(metaDataMap);
    }

    /**
     * refreshing the block list and trust list
     *
     * @param mode      {@value CheckBlackWhiteMode#API_DESCRIPTION}
     * @param tableList tableList
     */
    @Operation(summary = "refreshing the block list and trust list")
    @PostMapping("/extract/refresh/black/white/list")
    void refreshBlackWhiteList(@RequestParam CheckBlackWhiteMode mode, @RequestBody List<String> tableList) {
        metaDataService.refreshBlackWhiteList(mode, tableList);
    }

    /**
     * source endpoint extraction task construction
     *
     * @param processNo execution process no
     * @throws ProcessMultipleException the data extraction service is being executed for the current instance.
     *                                  new verification process cannot be enabled.
     */
    @Operation(summary = "construction a data extraction task for the current endpoint")
    @PostMapping("/extract/build/task/all")
    public Result<List<ExtractTask>> buildExtractTaskAllTables(
        @Parameter(name = "processNo", description = "execution process no") @RequestParam(name = "processNo")
            String processNo) {
        return Result.success(dataExtractService.buildExtractTaskAllTables(processNo));
    }

    /**
     * sink endpoint task configuration
     *
     * @param processNo execution process no
     * @param taskList  task list
     * @throws ProcessMultipleException the data extraction service is being executed for the current instance.
     *                                  new verification process cannot be enabled.
     */
    @Operation(summary = "sink endpoint task configuration")
    @PostMapping("/extract/config/sink/task/all")
    Result<Void> buildExtractTaskAllTables(
        @Parameter(name = "processNo", description = "execution process no") @RequestParam(name = "processNo")
            String processNo, @RequestBody List<ExtractTask> taskList) {
        dataExtractService.buildExtractTaskAllTables(processNo, taskList);
        return Result.success();
    }

    /**
     * full extraction service processing flow:
     * 1、create task information based on the table name
     * 2、build task thread based on task information
     * 2.1 thread pool configuration
     * 2.2 task thread construction
     * 3、extract data
     * 3.1 JDBC extraction, data processing, and data hash calculation
     * 3.2 data encapsulation ,pushing kafka
     * <p>
     * execution a table data extraction task
     *
     * @param processNo execution process no
     * @throws TaskNotFoundException if the task data is empty,the TaskNotFoundException exception is thrown.
     */
    @Operation(summary = "execute the data extraction task that has been created for the current endpoint")
    @PostMapping("/extract/exec/task/all")
    public Result<Void> execExtractTaskAllTables(
        @Parameter(name = "processNo", description = "execution process no") @RequestParam(name = "processNo")
            String processNo) {
        dataExtractService.execExtractTaskAllTables(processNo);
        return Result.success();
    }

    /**
     * clear the cached task information of the corresponding endpoint and rest the task.
     *
     * @return interface invoking result
     */
    @Operation(summary = " clear the cached task information of the corresponding endpoint and rest the task.")
    @PostMapping("/extract/clean/build/task")
    public Result<Void> cleanBuildedTask() {
        dataExtractService.cleanBuildTask();
        return Result.success();
    }

    /**
     * queries information about data extraction tasks in a specified table in the current process.
     *
     * @param tableName table name
     * @return table data extraction task information
     */
    @GetMapping("/extract/table/info")
    @Operation(summary = "queries information about data extraction tasks in a specified table in the current process.")
    Result<ExtractTask> queryTableInfo(
        @Parameter(name = "tableName", description = "table name") @RequestParam(name = "tableName") String tableName) {
        return Result.success(dataExtractService.queryTableInfo(tableName));
    }

    /**
     * DML statements required to generate a repair report
     *
     * @param tableName table name
     * @param dml       dml type
     * @param diffSet   primary key set
     * @return DML statement
     */
    @Operation(summary = "DML statements required to generate a repair report")
    @PostMapping("/extract/build/repairDML")
    Result<List<String>> buildRepairDml(
        @NotEmpty(message = "the schema to which the table to be repaired belongs cannot be empty")
        @RequestParam(name = "schema") String schema,
        @NotEmpty(message = "the name of the table to be repaired belongs cannot be empty")
        @RequestParam(name = "tableName") String tableName,
        @NotNull(message = "the DML type to be repaired belongs cannot be empty") @RequestParam(name = "dml") DML dml,
        @NotEmpty(message = "the primary key set to be repaired belongs cannot be empty") @RequestBody
            Set<String> diffSet) {
        return Result.success(dataExtractService.buildRepairDml(schema, tableName, dml, diffSet));
    }

    /**
     * querying table data
     *
     * @param tableName       table name
     * @param compositeKeySet primary key set
     * @return table record data
     */
    @Operation(summary = "querying table data")
    @PostMapping("/extract/query/table/data")
    Result<List<Map<String, String>>> queryTableColumnValues(
        @NotEmpty(message = "the name of the table to be repaired belongs cannot be empty")
        @RequestParam(name = "tableName") String tableName,
        @NotEmpty(message = "the primary key set to be repaired belongs cannot be empty") @RequestBody
            Set<String> compositeKeySet) {
        return Result.success(dataExtractService.queryTableColumnValues(tableName, new ArrayList<>(compositeKeySet)));
    }

    /**
     * creating an incremental extraction task based on data change logs
     *
     * @param sourceDataLogList data change logs list
     * @return interface invoking result
     */
    @Operation(summary = "creating an incremental extraction task based on data change logs")
    @PostMapping("/extract/increment/logs/data")
    Result<Void> notifyIncrementDataLogs(
        @RequestBody @NotNull(message = "Data change log cannot be empty") List<SourceDataLog> sourceDataLogList) {
        dataExtractService.buildExtractIncrementTaskByLogs(sourceDataLogList);
        dataExtractService.execExtractIncrementTaskByLogs();
        return Result.success();
    }

    @Operation(summary = "query the metadata of the current table structure and perform hash calculation.")
    @PostMapping("/extract/query/table/metadata/hash")
    Result<TableMetadataHash> queryTableMetadataHash(@RequestParam(name = "tableName") String tableName) {
        return Result.success(dataExtractService.queryTableMetadataHash(tableName));
    }

    /**
     * queries data corresponding to a specified primary key value in a table
     * and performs hash for secondary verification data query.
     *
     * @param dataLog data change logs
     * @return row data hash
     */
    @PostMapping("/extract/query/secondary/data/row/hash")
    Result<List<RowDataHash>> querySecondaryCheckRowData(@RequestBody SourceDataLog dataLog) {
        return Result.success(dataExtractService.querySecondaryCheckRowData(dataLog));
    }

    /**
     * queryDatabaseSchema
     *
     * @return DatabaseSchema
     */
    @GetMapping("/extract/query/database/schema")
    Result<String> getDatabaseSchema() {
        return Result.success(dataExtractService.queryDatabaseSchema());
    }
}
