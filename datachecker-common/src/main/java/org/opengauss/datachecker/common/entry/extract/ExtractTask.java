package org.opengauss.datachecker.common.entry.extract;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

@ToString
@Data
@Accessors(chain = true)
public class ExtractTask {
    /**
     * 任务名称
     */
    private String taskName;
    /**
     * 表名称
     */
    private String tableName;

    /**
     * 任务分拆总数：1 表示未分拆，大于1则表示分拆为divisionsTotalNumber个任务
     */
    private int divisionsTotalNumber;
    /**
     * 当前表，拆分任务序列
     */
    private int divisionsOrdinal;
    /**
     * 任务执行起始位置
     */
    private long start;
    /**
     * 任务执行偏移量
     */
    private long offset;
    /**
     * 表元数据信息
     */
    private TableMetadata tableMetadata;

    public boolean isDivisions() {
        return divisionsTotalNumber > 1;
    }
}
