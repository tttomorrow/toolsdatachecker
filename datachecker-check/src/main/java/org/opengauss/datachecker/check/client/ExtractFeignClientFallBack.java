//package org.opengauss.datachecker.check.client;
//
//import lombok.extern.slf4j.Slf4j;
//import org.opengauss.datachecker.common.entry.check.IncrementCheckConifg;
//import org.opengauss.datachecker.common.entry.enums.CheckBlackWhiteMode;
//import org.opengauss.datachecker.common.entry.enums.DML;
//import org.opengauss.datachecker.common.entry.extract.*;
//import org.opengauss.datachecker.common.web.Result;
//
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
///**
// * @author ：wangchao
// * @date ：Created in 2022/5/30
// * @since ：11
// */
//@Slf4j
//public class ExtractFeignClientFallBack {
//    public ExtractFeignClient getClient(Throwable throwable) {
//        return new ExtractFeignClient() {
//            /**
//             * 服务健康检查
//             *
//             * @return 返回接口相应结果
//             */
//            @Override
//            public Result<Void> health() {
//                log.error("health check error：{}", throwable.getMessage());
//                return Result.error("health check error");
//            }
//
//            /**
//             * 端点加载元数据信息
//             *
//             * @return 返回元数据
//             */
//            @Override
//            public Result<Map<String, TableMetadata>> queryMetaDataOfSchema() {
//                log.error("query database metadata error ：{}", throwable.getMessage());
//                return Result.error("query database metadata error");
//            }
//
//            /**
//             * 抽取任务构建
//             *
//             * @param processNo 执行进程编号
//             */
//            @Override
//            public Result<List<ExtractTask>> buildExtractTaskAllTables(String processNo) {
//                log.error("build extract task error , process number ={} ：{}", processNo, throwable.getMessage());
//                return Result.error(String.format("build extract task error : process number =%s", processNo));
//            }
//
//            /**
//             * 宿端抽取任务配置
//             *
//             * @param processNo 执行进程编号
//             * @param taskList  源端任务列表
//             * @return 请求结果
//             */
//            @Override
//            public Result<Void> buildExtractTaskAllTables(String processNo, List<ExtractTask> taskList) {
//                return null;
//            }
//
//            /**
//             * 全量抽取业务处理流程
//             *
//             * @param processNo 执行进程序号
//             * @return 执行结果
//             */
//            @Override
//            public Result<Void> execExtractTaskAllTables(String processNo) {
//                log.error("runing extract task error , process number ={} ：{}", processNo, throwable.getMessage());
//                return Result.error(String.format("runing extract task error : process number =%s", processNo));
//            }
//
//
//            @Override
//            public Result<Topic> queryTopicInfo(String tableName) {
//                return null;
//            }
//
//            @Override
//            public Result<Topic> getIncrementTopicInfo(String tableName) {
//                return null;
//            }
//
//            /**
//             * 查询指定topic数据
//             * @param tableName  topic名称
//             * @param partitions topic分区
//             * @return topic数据
//             */
//            @Override
//            public Result<List<RowDataHash>> queryTopicData(String tableName, int partitions) {
//                return null;
//            }
//
//            /**
//             * 查询指定增量topic数据
//             *
//             * @param tableName 表名称
//             * @return topic数据
//             */
//            @Override
//            public Result<List<RowDataHash>> queryIncrementTopicData(String tableName) {
//                return null;
//            }
//
//            /**
//             * 清理对端环境
//             *
//             * @param processNo 执行进程序号
//             * @return 执行结果
//             */
//            @Override
//            public Result<Void> cleanEnvironment(String processNo) {
//                log.error("clean environment error , process number ={} : ", processNo, throwable);
//                return null;
//            }
//
//            @Override
//            public Result<Void> cleanTask() {
//                return null;
//            }
//
//            @Override
//            public Result<List<String>> buildRepairDml(String schema, String tableName, DML dml, Set<String> diffSet) {
//                log.error("build Repair DML  error , tableName=[{}] dml=[{}] diffs=[{}] :", tableName, dml.getDescription(), diffSet, throwable);
//                return null;
//            }
//
//            @Override
//            public void notifyIncrementDataLogs(List<SourceDataLog> dataLogList) {
//
//            }
//
//            @Override
//            public Result<TableMetadataHash> queryTableMetadataHash(String tableName) {
//                return null;
//            }
//
//            @Override
//            public Result<List<RowDataHash>> querySecondaryCheckRowData(SourceDataLog dataLog) {
//                return null;
//            }
//
//            @Override
//            public Result<String> getDatabaseSchema() {
//                return null;
//            }
//
//            @Override
//            public void refushBlackWhiteList(CheckBlackWhiteMode mode, List<String> whiteList) {
//
//            }
//
//            @Override
//            public Result<Void> configIncrementCheckEnvironment(IncrementCheckConifg conifg) {
//                return null;
//            }
//        };
//
//    }
//}
