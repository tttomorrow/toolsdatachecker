package org.opengauss.datachecker.common.entry.extract;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

@Data
@EqualsAndHashCode
@Accessors(chain = true)
public class RowDataHash {

    /**
     * 主键为数字类型则 转字符串，表主键为联合主键，则当前属性为表主键联合字段对应值 拼接字符串 以下划线拼接
     */
    private String primaryKey;

    /**
     * 主键对应值的哈希值
     */
    private long primaryKeyHash;
    /**
     * 当前记录的总体哈希值
     */
    private long rowHash;
}
