package org.opengauss.datachecker.common.entry.extract;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;

/**
 * 表元数据信息
 */
@Data
@Accessors(chain = true)
@ToString
public class ColumnsMetaData {
    /**
     * 表名
     */
    private String tableName;
    /**
     * 主键列名称
     */
    private String columnName;
    /**
     * 主键列数据类型
     */
    private String columnType;
    /**
     * 主键列数据类型
     */
    private String dataType;
    /**
     * 主键表序号
     */
    private int ordinalPosition;
    /**
     * 主键
     */
    private ColumnKey columnKey;
}

