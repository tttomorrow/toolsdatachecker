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
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.util.IdGenerator;
import org.opengauss.datachecker.common.web.Result;
import org.opengauss.datachecker.extract.kafka.KafkaConsumerService;
import org.opengauss.datachecker.extract.kafka.KafkaManagerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Data extraction service: Kafka management service
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/23
 * @since ：11
 */
@Tag(name = "KafkaManagerController", description = "Data extraction service: Kafka management service")
@RestController
public class KafkaManagerController {
    @Autowired
    private KafkaManagerService kafkaManagerService;
    @Autowired
    private KafkaConsumerService kafkaConsumerService;

    /**
     * Query for specified topic data
     *
     * @param tableName  table Name
     * @param partitions topic partition
     * @return topic data
     */
    @Operation(summary = "Query for specified topic data")
    @GetMapping("/extract/query/topic/data")
    public Result<List<RowDataHash>> queryTopicData(
        @Parameter(name = "tableName", description = "table Name") @RequestParam("tableName") String tableName,
        @Parameter(name = "partitions", description = "kafka partition number") @RequestParam("partitions")
            int partitions) {
        return Result.success(kafkaConsumerService.getTopicRecords(tableName, partitions));
    }

    /**
     * Query the specified incremental topic data
     *
     * @param tableName tableName
     * @return topic data
     */
    @Operation(summary = "Query the specified incremental topic data")
    @GetMapping("/extract/query/increment/topic/data")
    public Result<List<RowDataHash>> queryIncrementTopicData(
        @Parameter(name = "tableName", description = "tableName") @RequestParam("tableName") String tableName) {
        return Result.success(kafkaConsumerService.getIncrementTopicRecords(tableName));
    }

    /**
     * Create topic according to the table name
     *
     * @param tableName  tableName
     * @param partitions partitions
     * @return Topic name after successful creation
     */
    @Operation(summary = "Create topic according to the table name", description = "Used to test Kafka topic creation")
    @PostMapping("/extract/create/topic")
    public Result<String> createTopic(
        @Parameter(name = "tableName", description = "tableName") @RequestParam("tableName") String tableName,
        @Parameter(name = "partitions", description = "Total number of Kafka partitions") @RequestParam("partitions")
            int partitions) {
        String process = IdGenerator.nextId36();
        return Result.success(kafkaManagerService.createTopic(process, tableName, partitions));
    }

    /**
     * Query topic information of the specified table name
     *
     * @param tableName tableName
     * @return kafka topic info
     */
    @Operation(summary = "Query topic information of the specified table name")
    @GetMapping("/extract/topic/info")
    public Result<Topic> queryTopicInfo(
        @Parameter(name = "tableName", description = "tableName") @RequestParam(name = "tableName") String tableName) {
        return Result.success(kafkaManagerService.getTopic(tableName));
    }

    /**
     * Query topic information of the specified table name
     *
     * @param tableName tableName
     * @return kafka topic info
     */
    @Operation(summary = "Query topic information of the specified table name")
    @GetMapping("/extract/increment/topic/info")
    public Result<Topic> getIncrementTopicInfo(
        @Parameter(name = "tableName", description = "tableName") @RequestParam(name = "tableName") String tableName) {
        return Result.success(kafkaManagerService.getIncrementTopicInfo(tableName));
    }

    /**
     * Clean up all topics related to data extraction
     *
     * @param processNo processNo
     * @return request result
     */
    @Operation(summary = "Clean up all topics related to data extraction")
    @PostMapping("/extract/delete/topic/history")
    public Result<Void> deleteTopic(
        @Parameter(name = "processNo", description = "processNo") @RequestParam(name = "processNo") String processNo) {
        kafkaManagerService.deleteTopic(processNo);
        return Result.success();
    }

    /**
     * Clean up all topics related to data extraction
     *
     * @return request result
     */
    @Operation(summary = "Clean up all topics related to data extraction", description = "Clean up all topics in Kafka")
    @PostMapping("/extract/super/delete/topic/history")
    public Result<Void> deleteTopic() {
        kafkaManagerService.deleteTopic();
        return Result.success();
    }

    /**
     * Delete the topic specified in Kafka
     *
     * @param topicName topicName
     * @return request result
     */
    @Operation(summary = "Delete the topic specified in Kafka", description = "Delete the topic specified in Kafka")
    @PostMapping("/extract/delete/topic")
    public Result<Void> deleteTopicHistory(
        @Parameter(name = "topicName", description = "topic Name") @RequestParam(name = "topicName") String topicName) {
        kafkaManagerService.deleteTopicByName(topicName);
        return Result.success();
    }
}
