package org.opengauss.datachecker.check.client;

import org.opengauss.datachecker.common.entry.check.IncrementCheckConifg;
import org.opengauss.datachecker.common.entry.enums.CheckBlackWhiteMode;
import org.opengauss.datachecker.common.entry.enums.DML;
import org.opengauss.datachecker.common.entry.extract.*;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
public interface ExtractFeignClient {
    /**
     * 服务健康检查
     *
     * @return 返回接口相应结果
     */
    @GetMapping("/extract/health")
    Result<Void> health();

    /**
     * 端点加载元数据信息
     *
     * @return 返回元数据
     */
    @GetMapping("/extract/load/database/meta/data")
    Result<Map<String, TableMetadata>> queryMetaDataOfSchema();

    /**
     * 抽取任务构建
     *
     * @param processNo 执行进程编号
     * @return 返回构建任务集合
     */
    @PostMapping("/extract/build/task/all")
    Result<List<ExtractTask>> buildExtractTaskAllTables(@RequestParam(name = "processNo") String processNo);

    /**
     * 宿端抽取任务配置
     *
     * @param processNo 执行进程编号
     * @param taskList  源端任务列表
     * @return 请求结果
     */
    @PostMapping("/extract/config/sink/task/all")
    Result<Void> buildExtractTaskAllTables(@RequestParam(name = "processNo") String processNo,
                                           @RequestBody List<ExtractTask> taskList);

    /**
     * 全量抽取业务处理流程
     *
     * @param processNo 执行进程序号
     * @return 执行结果
     */
    @PostMapping("/extract/exec/task/all")
    Result<Void> execExtractTaskAllTables(@RequestParam(name = "processNo") String processNo);


    /**
     * 查询指定表对应Topic信息
     *
     * @param tableName 表名称
     * @return Topic信息
     */
    @GetMapping("/extract/topic/info")
    Result<Topic> queryTopicInfo(@RequestParam(name = "tableName") String tableName);

    /**
     * 获取增量 Topic信息
     *
     * @param tableName 表名称
     * @return 返回表对应的Topic信息
     */
    @GetMapping("/extract/increment/topic/info")
    Result<Topic> getIncrementTopicInfo(@RequestParam(name = "tableName") String tableName);

    /**
     * 查询指定topic数据
     *
     * @param tableName  表名称
     * @param partitions topic分区
     * @return topic数据
     */
    @GetMapping("/extract/query/topic/data")
    Result<List<RowDataHash>> queryTopicData(@RequestParam("tableName") String tableName,
                                             @RequestParam("partitions") int partitions);

    /**
     * 查询指定增量topic数据
     *
     * @param tableName 表名称
     * @return topic数据
     */
    @GetMapping("/extract/query/increment/topic/data")
    Result<List<RowDataHash>> queryIncrementTopicData(@RequestParam("tableName") String tableName);

    /**
     * 清理对端环境
     *
     * @param processNo 执行进程序号
     * @return 执行结果
     */
    @PostMapping("/extract/clean/environment")
    Result<Void> cleanEnvironment(@RequestParam(name = "processNo") String processNo);

    /**
     * 清除抽取端 任务缓存
     *
     * @return 执行结果
     */
    @PostMapping("/extract/clean/task")
    Result<Void> cleanTask();

    /**
     * 根据参数构建修复语句
     *
     * @param schema    待修复端DB对应schema
     * @param tableName 表名称
     * @param dml       修复类型{@link DML}
     * @param diffSet   差异主键集合
     * @return 返回修复语句集合
     */
    @PostMapping("/extract/build/repairDML")
    Result<List<String>> buildRepairDml(@RequestParam(name = "schema") String schema,
                                        @RequestParam(name = "tableName") String tableName,
                                        @RequestParam(name = "dml") DML dml,
                                        @RequestBody Set<String> diffSet);

    /**
     * 下发增量日志数据
     *
     * @param dataLogList 增量数据日志
     */
    @PostMapping("/extract/increment/logs/data")
    void notifyIncrementDataLogs(List<SourceDataLog> dataLogList);

    /**
     * 查询表元数据哈希信息
     *
     * @param tableName 表名称
     * @return 表元数据哈希
     */
    @PostMapping("/extract/query/table/metadata/hash")
    Result<TableMetadataHash> queryTableMetadataHash(@RequestParam(name = "tableName") String tableName);

    /**
     * 提取增量日志数据记录
     *
     * @param dataLog 日志记录
     * @return 返回抽取结果
     */
    @PostMapping("/extract/query/secondary/data/row/hash")
    Result<List<RowDataHash>> querySecondaryCheckRowData(@RequestBody SourceDataLog dataLog);

    /**
     * 查询抽取端数据库schema信息
     *
     * @return 返回schema
     */
    @GetMapping("/extract/query/database/schema")
    Result<String> getDatabaseSchema();

    /**
     * 更新黑白名单列表
     *
     * @param mode      黑白名单模式枚举{@linkplain CheckBlackWhiteMode}
     * @param tableList 黑白名单列表-表名称集合
     */
    @PostMapping("/extract/refush/black/white/list")
    void refushBlackWhiteList(@RequestParam CheckBlackWhiteMode mode, @RequestBody List<String> tableList);

    /**
     * 配置增量校验场景 debezium相关配置信息
     *
     * @param conifg debezium相关配置
     * @return 返回请求结果
     */
    @PostMapping("/extract/debezium/topic/config")
    Result<Void> configIncrementCheckEnvironment(IncrementCheckConifg conifg);
}
