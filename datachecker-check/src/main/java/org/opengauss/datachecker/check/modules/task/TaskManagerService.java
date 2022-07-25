package org.opengauss.datachecker.check.modules.task;

import org.opengauss.datachecker.common.entry.enums.Endpoint;

import java.util.List;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
public interface TaskManagerService {
    /**
     * 刷新指定任务的数据抽取表执行状态
     *
     * @param tableName 表名称
     * @param endpoint 端点类型 {@link org.opengauss.datachecker.common.entry.enums.Endpoint}
     */
    void refushTableExtractStatus(String tableName, Endpoint endpoint);


    /**
     * 初始化任务状态
     *
     * @param tableNameList 表名称列表
     */
    void initTableExtractStatus(List<String> tableNameList);

    /**
     * 清理任务状态信息
     */
    void cleanTaskStatus();
}
