package org.opengauss.datachecker.common.entry.extract;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

@Data
@EqualsAndHashCode
@Accessors(chain = true)
public class TableMetadataHash {

    /**
     * 表名
     */
    private String tableName;

    /**
     * 当前记录的总体哈希值
     */
    private long tableHash;
}
