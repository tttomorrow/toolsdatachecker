package org.opengauss.datachecker.common.entry.extract;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.experimental.Accessors;
import org.opengauss.datachecker.common.constant.Constants;

import java.util.List;

/**
 * 源端数据变更日志
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/14
 * @since ：11
 */
@Schema(description = "源端数据变更日志")
@Data
@Accessors(chain = true)
public class SourceDataLog {

    private static final String PRIMARY_DELIMITER = Constants.PRIMARY_DELIMITER;
    /**
     * 数据变更日志 对应表名称
     */
    @Schema(name = "tableName", description = "表名称")
    private String tableName;

    /**
     * 当前表的主键字段名称列表
     */
    @Schema(name = "compositePrimarys", description = "当前表的主键字段名称列表")
    private List<String> compositePrimarys;

    /**
     * 相同数据操作类型{@code operateCategory}的数据变更的主键值列表 <p>
     * 单主键表 ：主键值直接添加进{@code compositePrimarysValues}集合。<p>
     * 复合主键：对主键值进行组装，根据{@code compositePrimarys}记录的主键字段顺序，进行拼接。链接符{@value PRIMARY_DELIMITER}
     */
    @Schema(name = "compositePrimaryValues", description = "相同数据操作类型{@code operateCategory}的数据变更的主键值列表")
    private List<String> compositePrimaryValues;
}
