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
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.AllArgsConstructor;
import org.opengauss.datachecker.common.entry.check.IncrementCheckConfig;
import org.opengauss.datachecker.common.entry.check.IncrementCheckTopic;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.web.Result;
import org.opengauss.datachecker.extract.debe.DataConsolidationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * incremental verification debezium data integration
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/30
 * @since ：11
 */
@Tag(name = "incremental verification debezium data integration")
@RestController
@AllArgsConstructor
public class DataConsolidationController {
    private final DataConsolidationService dataConsolidationService;

    /**
     * Querying topic records
     *
     * @param topicName topicName
     * @return topic records
     */
    @Operation(summary = "Querying topic records")
    @GetMapping("/extract/debezium/topic/records")
    Result<List<SourceDataLog>> getDebeziumTopicRecords(@RequestParam(name = "topicName") String topicName) {
        return Result.success(dataConsolidationService.getDebeziumTopicRecords(topicName));
    }

    /**
     * queries the topic information of the debezium
     *
     * @return topic information
     */
    @Operation(summary = "queries the topic information of the debezium")
    @PostMapping("/extract/debezium/topic/count")
    Result<IncrementCheckTopic> getDebeziumTopicRecordCount() {
        return Result.success(dataConsolidationService.getDebeziumTopicRecordOffSet());
    }

    /**
     * configuring debezium-related environment information
     *
     * @param config config
     * @return request result
     */
    @Operation(summary = "configuring debezium-related environment information")
    @PostMapping("/extract/debezium/topic/config")
    Result<Void> configIncrementCheckEnvironment(@RequestBody IncrementCheckConfig config) {
        dataConsolidationService.configIncrementCheckEnvironment(config);
        return Result.success();
    }
}
