package org.opengauss.datachecker.common.entry.extract;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/14
 * @since ：11
 */
@ToString
@Data
@Accessors(chain = true)
public class ExtractIncrementTask {
    /**
     * 表名称
     */
    private String tableName;
    /**
     * 当前抽取端点 schema
     */
    private String schema;
    /**
     * 任务名称
     */
    private String taskName;

    /**
     * 数据变更日志
     */
    private SourceDataLog sourceDataLog;
}
