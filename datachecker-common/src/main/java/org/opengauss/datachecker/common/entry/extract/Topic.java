package org.opengauss.datachecker.common.entry.extract;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

@ToString
@Data
@Accessors(chain = true)
public class Topic {
    /**
     * 表名称
     */
    private String tableName;
    /**
     * 当前表，对应的Topic名称
     */
    private String topicName;
    /**
     * 当前表存在在Kafka Topic中的数据的分区总数
     */
    private int partitions;

}
