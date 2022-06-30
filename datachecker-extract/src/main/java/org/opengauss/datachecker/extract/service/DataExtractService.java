package org.opengauss.datachecker.extract.service;

import org.opengauss.datachecker.common.entry.enums.DML;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadataHash;
import org.opengauss.datachecker.common.exception.ProcessMultipleException;
import org.opengauss.datachecker.common.exception.TaskNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author wang chao
 * @description 数据抽取服务
 * @date 2022/5/8 19:27
 * @since 11
 **/
public interface DataExtractService {

    /**
     * 抽取任务构建
     *
     * @param processNo 执行进程编号
     * @return 指定processNo下 构建抽取任务集合
     * @throws ProcessMultipleException 当前实例正在执行数据抽取服务，不能重新开启新的校验。
     */
    List<ExtractTask> buildExtractTaskAllTables(String processNo) throws ProcessMultipleException;

    /**
     * 宿端任务配置
     *
     * @param processNo 执行进程编号
     * @param taskList  任务列表
     * @throws ProcessMultipleException 前实例正在执行数据抽取服务，不能重新开启新的校验。
     */
    void buildExtractTaskAllTables(String processNo, List<ExtractTask> taskList) throws ProcessMultipleException;

    /**
     * 执行表数据抽取任务
     *
     * @param processNo 执行进程编号
     * @throws TaskNotFoundException 任务数据为空，则抛出异常 TaskNotFoundException
     */
    void execExtractTaskAllTables(String processNo) throws TaskNotFoundException;

    /**
     * 清理当前构建任务
     */
    void cleanBuildedTask();

    /**
     * 查询当前流程下，指定名称的详细任务信息
     *
     * @param taskName 任务名称
     * @return 任务详细信息，若不存在返回{@code null}
     */
    ExtractTask queryTableInfo(String taskName);

    /**
     * 生成修复报告的DML语句
     *
     * @param schema    schema信息
     * @param tableName 表名
     * @param dml       dml 类型
     * @param diffSet   待生成主键集合
     * @return DML语句
     */
    List<String> buildRepairDml(String schema, String tableName, DML dml, Set<String> diffSet);

    /**
     * 查询表数据
     *
     * @param tableName       表名称
     * @param compositeKeySet 复核主键集合
     * @return 主键对应表数据
     */
    List<Map<String, String>> queryTableColumnValues(String tableName, List<String> compositeKeySet);

    /**
     * 根据数据变更日志 构建增量抽取任务
     *
     * @param sourceDataLogs 数据变更日志
     */
    void buildExtractIncrementTaskByLogs(List<SourceDataLog> sourceDataLogs);

    /**
     * 执行增量校验数据抽取
     */
    void execExtractIncrementTaskByLogs();

    /**
     * 查询当前表结构元数据信息，并进行Hash
     *
     * @param tableName 表名称
     * @return 表结构Hash
     */
    TableMetadataHash queryTableMetadataHash(String tableName);

    /**
     * 查询表指定PK列表数据，并进行Hash 用于二次校验数据查询
     *
     * @param dataLog 数据日志
     * @return rowdata hash
     */
    List<RowDataHash> querySecondaryCheckRowData(SourceDataLog dataLog);

    /**
     * 查询当前链接数据库 的schema
     *
     * @return 数据库的schema
     */
    String queryDatabaseSchema();
}
