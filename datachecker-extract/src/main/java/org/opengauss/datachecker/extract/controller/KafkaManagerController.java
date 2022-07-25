package org.opengauss.datachecker.extract.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.util.IdWorker;
import org.opengauss.datachecker.common.web.Result;
import org.opengauss.datachecker.extract.kafka.KafkaConsumerService;
import org.opengauss.datachecker.extract.kafka.KafkaManagerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;


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
    public Result<List<RowDataHash>> queryTopicData(@Parameter(name = "tableName", description = "table Name")
                                                    @RequestParam("tableName") String tableName,
                                                    @Parameter(name = "partitions", description = "kafka partition number")
                                                    @RequestParam("partitions") int partitions) {
        return Result.success(kafkaConsumerService.getTopicRecords(tableName, partitions));
    }

    /**
     * 查询指定增量topic数据
     *
     * @param tableName 表名称
     * @return topic数据
     */
    @Operation(summary = "查询指定增量topic数据")
    @GetMapping("/extract/query/increment/topic/data")
    public Result<List<RowDataHash>> queryIncrementTopicData(@Parameter(name = "tableName", description = "表名称")
                                                             @RequestParam("tableName") String tableName) {
        return Result.success(kafkaConsumerService.getIncrementTopicRecords(tableName));
    }

    /**
     * 根据表名称，创建topic
     *
     * @param tableName  表名称
     * @param partitions 分区总数
     * @return 创建成功后的topic名称
     */
    @Operation(summary = "根据表名称创建topic", description = "用于测试kafka topic创建")
    @PostMapping("/extract/create/topic")
    public Result<String> createTopic(@Parameter(name = "tableName", description = "表名称")
                                      @RequestParam("tableName") String tableName,
                                      @Parameter(name = "partitions", description = "kafka分区总数")
                                      @RequestParam("partitions") int partitions) {
        String process = IdWorker.nextId36();
        return Result.success(kafkaManagerService.createTopic(process, tableName, partitions));
    }

    /**
     * 查询所有的topic名称列表
     *
     * @return topic名称列表
     */
    @Operation(summary = "查询当前端点所有的topic名称列表")
    @GetMapping("/extract/query/topic")
    public Result<List<String>> queryTopicData() {
        return Result.success(kafkaManagerService.getAllTopic());
    }

    @Operation(summary = "查询指定表名的Topic信息")
    @GetMapping("/extract/topic/info")
    public Result<Topic> queryTopicInfo(@Parameter(name = "tableName", description = "表名称")
                                        @RequestParam(name = "tableName") String tableName) {
        return Result.success(kafkaManagerService.getTopic(tableName));
    }

    @Operation(summary = "查询指定表名的Topic信息")
    @GetMapping("/extract/increment/topic/info")
    public Result<Topic> getIncrementTopicInfo(@Parameter(name = "tableName", description = "表名称")
                                               @RequestParam(name = "tableName") String tableName) {
        return Result.success(kafkaManagerService.getIncrementTopicInfo(tableName));
    }

    @Operation(summary = "清理所有数据抽取相关topic", description = "清理kafka中 前缀TOPIC_EXTRACT_Endpoint_process_ 的所有Topic")
    @PostMapping("/extract/delete/topic/history")
    public Result<Void> deleteTopic(@Parameter(name = "processNo", description = "校验流程号")
                                    @RequestParam(name = "processNo") String processNo) {
        kafkaManagerService.deleteTopic(processNo);
        return Result.success();
    }

    @Operation(summary = "清理所有数据抽取相关topic", description = "清理kafka中 前缀TOPIC_EXTRACT_Endpoint_ 的所有Topic")
    @PostMapping("/extract/super/delete/topic/history")
    public Result<Void> deleteTopic() {
        kafkaManagerService.deleteTopic();
        return Result.success();
    }

    @Operation(summary = "删除kafka中指定topic", description = "删除kafka中指定topic")
    @PostMapping("/extract/delete/topic")
    public Result<Void> deleteTopicHistory(@Parameter(name = "topicName", description = "topic名称")
                                           @RequestParam(name = "topicName") String topicName) {
        kafkaManagerService.deleteTopicByName(topicName);
        return Result.success();
    }
}
