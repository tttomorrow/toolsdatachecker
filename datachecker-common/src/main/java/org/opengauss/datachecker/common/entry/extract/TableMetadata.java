package org.opengauss.datachecker.common.entry.extract;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * 表元数据信息
 */
@Schema(name = "表元数据信息")
@Data
@Accessors(chain = true)
@ToString
public class TableMetadata {

    /**
     * 表名
     */
    private String tableName;
    /**
     * 表数据总量
     */
    private long tableRows;

    /**
     * 主键列属性
     */
    private List<ColumnsMetaData> primaryMetas;

    /**
     * 表列属性
     */
    private List<ColumnsMetaData> columnsMetas;

}

