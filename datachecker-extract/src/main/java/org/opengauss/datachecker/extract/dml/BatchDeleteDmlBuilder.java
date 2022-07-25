package org.opengauss.datachecker.extract.dml;

import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/13
 * @since ：11
 */
public class BatchDeleteDmlBuilder extends DmlBuilder {

    /**
     * 构建 Schema
     *
     * @param schema Schema
     * @return DeleteDMLBuilder 构建器
     */
    public BatchDeleteDmlBuilder schema(@NotNull String schema) {
        super.buildSchema(schema);
        return this;
    }
    /**
     * 构建 tableName
     *
     * @param tableName tableName
     * @return DeleteDMLBuilder 构建器
     */
    public BatchDeleteDmlBuilder tableName(@NotNull String tableName) {
        super.buildTableName(tableName);
        return this;
    }

    /**
     * 生成单一主键字段 delete from schema.table where pk in (参数...) 条件语句
     *
     * @param primaryMeta 主键元数据
     * @return DeleteDMLBuilder 构建器
     */
    public BatchDeleteDmlBuilder conditionPrimary(@NonNull ColumnsMetaData primaryMeta) {
        Assert.isTrue(StringUtils.isNotEmpty(primaryMeta.getColumnName()), "表元数据主键字段名称为空");
        this.condition = primaryMeta.getColumnName().concat(IN);
        return this;
    }
    /**
     * 构建复合主键参数的条件查询语句<p>
     * select columns... from table where (pk1,pk2) in ((pk1_val,pk2_val),(pk1_val,pk2_val))<p>
     *
     * @param primaryMeta
     * @return SelectDMLBuilder构建器
     */
    public BatchDeleteDmlBuilder conditionCompositePrimary(@NonNull List<ColumnsMetaData> primaryMeta) {
        this.condition = buildConditionCompositePrimary(primaryMeta).concat(IN);
        return this;
    }

    public String build() {
        StringBuffer sb = new StringBuffer();
        sb.append(Fragment.DELETE).append(Fragment.FROM)
                .append(schema).append(Fragment.LINKER).append(tableName)
                .append(Fragment.WHERE).append(condition)
                .append(Fragment.END)
        ;
        return sb.toString();
    }
}
