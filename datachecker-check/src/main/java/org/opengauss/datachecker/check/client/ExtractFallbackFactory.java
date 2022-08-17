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

package org.opengauss.datachecker.check.client;

import org.opengauss.datachecker.common.entry.check.IncrementCheckConfig;
import org.opengauss.datachecker.common.entry.enums.CheckBlackWhiteMode;
import org.opengauss.datachecker.common.entry.enums.DML;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.TableMetadataHash;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ExtractFallbackFactory
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/25
 * @since ：11
 */
@Component
public class ExtractFallbackFactory implements FallbackFactory<ExtractFeignClient> {
    /**
     * Returns an instance of the fallback appropriate for the given cause.
     *
     * @param cause cause of an exception.
     * @return fallback
     */
    @Override
    public ExtractFeignClient create(Throwable cause) {
        return new ExtractFeignClientImpl();
    }

    private class ExtractFeignClientImpl implements ExtractFeignClient {
        @Override
        public Result<Void> health() {
            return Result.error("Remote call service health check exception");
        }

        @Override
        public Result<Map<String, TableMetadata>> queryMetaDataOfSchema() {
            return Result.error("Remote call, endpoint loading metadata information exception");
        }

        @Override
        public Result<List<ExtractTask>> buildExtractTaskAllTables(String processNo) {
            return Result.error("Remote call, extract task construction exception");
        }

        @Override
        public Result<Void> buildExtractTaskAllTables(String processNo, List<ExtractTask> taskList) {
            return Result.error("Remote call, abnormal configuration of the destination extraction task");
        }

        @Override
        public Result<Void> execExtractTaskAllTables(String processNo) {
            return Result.error("Remote call, full extraction business processing process exception");
        }

        @Override
        public Result<Topic> queryTopicInfo(String tableName) {
            return Result
                .error("Remote call, query the topic information corresponding to the specified table is abnormal");
        }

        @Override
        public Result<Topic> getIncrementTopicInfo(String tableName) {
            return Result.error("Remote call, get incremental topic information exception");
        }

        @Override
        public Result<List<RowDataHash>> queryTopicData(String tableName, int partitions) {
            return Result.error("Remote call, query the specified topic data exception");
        }

        @Override
        public Result<List<RowDataHash>> queryIncrementTopicData(String tableName) {
            return Result.error("Remote call, query the specified incremental topic data exception");
        }

        @Override
        public Result<Void> cleanEnvironment(String processNo) {
            return Result.error("Remote call, clean up the opposite end environment exception");
        }

        @Override
        public Result<Void> cleanTask() {
            return Result.error("Remote call, clear the task cache exception at the extraction end");
        }

        @Override
        public Result<List<String>> buildRepairDml(String schema, String tableName, DML dml, Set<String> diffSet) {
            return Result.error("Remote call, build and repair statement exceptions according to parameters");
        }

        @Override
        public void notifyIncrementDataLogs(List<SourceDataLog> dataLogList) {
        }

        @Override
        public Result<TableMetadataHash> queryTableMetadataHash(String tableName) {
            return Result.error("Remote call, query table metadata hash information exception");
        }

        @Override
        public Result<List<RowDataHash>> querySecondaryCheckRowData(SourceDataLog dataLog) {
            return Result.error("Remote call, query secondary verification increment log data exception");
        }

        @Override
        public Result<String> getDatabaseSchema() {
            return Result
                .error("Remote call, query the schema information of the database at the extraction end, abnormal“");
        }

        @Override
        public void refreshBlackWhiteList(CheckBlackWhiteMode mode, List<String> tableList) {

        }

        @Override
        public Result<Void> configIncrementCheckEnvironment(IncrementCheckConfig config) {
            return Result.error("Remote call, configuration incremental verification scenario, "
                + "configuration information related to debezium is abnormal");
        }
    }
}
