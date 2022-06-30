package org.opengauss.datachecker.common.entry.extract;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors
public class PrimaryMeta {
    /**
     * 主键列名称
     */
    private String columnName;
    /**
     * 主键列数据类型
     */
    private String columnType;
    /**
     * 主键表序号
     */
    private int ordinalPosition;
}
