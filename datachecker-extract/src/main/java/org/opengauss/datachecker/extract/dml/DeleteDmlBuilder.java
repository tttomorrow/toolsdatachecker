package org.opengauss.datachecker.extract.dml;

import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.extract.constants.ExtConstants;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.stream.IntStream;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/13
 * @since ：11
 */
public class DeleteDmlBuilder extends DmlBuilder {

    /**
     * 构建 Schema
     *
     * @param schema Schema
     * @return DeleteDMLBuilder 构建器
     */
    public DeleteDmlBuilder schema(@NotNull String schema) {
        super.buildSchema(schema);
        return this;
    }

    /**
     * 构建 tableName
     *
     * @param tableName tableName
     * @return DeleteDMLBuilder 构建器
     */
    public DeleteDmlBuilder tableName(@NotNull String tableName) {
        super.buildTableName(tableName);
        return this;
    }

    /**
     * 生成单一主键字段 delete from schema.table where pk = 参数 条件语句
     *
     * @param primaryMeta 主键元数据
     * @return DeleteDMLBuilder 构建器
     */
    public DeleteDmlBuilder condition(@NonNull ColumnsMetaData primaryMeta, String value) {
        Assert.isTrue(StringUtils.isNotEmpty(primaryMeta.getColumnName()), "表元数据主键字段名称为空");
        if (DIGITAL.contains(primaryMeta.getDataType())) {
            this.condition = primaryMeta.getColumnName().concat(EQUAL).concat(value);
        } else {
            this.condition = primaryMeta.getColumnName().concat(EQUAL)
                    .concat(SINGLE_QUOTES).concat(value).concat(SINGLE_QUOTES);
        }
        return this;
    }

    /**
     * 构建复合主键参数的条件 delete语句<p>
     * delete from schema.table where pk1 = pk1_val and pk2 = pk2_val<p>
     *
     * @param compositeKey 复合主键
     * @param primaryMetas 主键元数据
     * @return SelectDMLBuilder 构建器
     */
    public DeleteDmlBuilder conditionCompositePrimary(String compositeKey, List<ColumnsMetaData> primaryMetas) {
        this.condition = buildCondition(compositeKey, primaryMetas);
        return this;
    }

    /**
     * 构建主键过滤（where）条件 <p>
     * pk = pk_value <p>
     * or <p>
     * pk = 'pk_value' <p>
     *
     * @param compositeKey 复合主键
     * @param primaryMetas 主键元数据
     * @return 返回主键where条件
     */
    public String buildCondition(String compositeKey, List<ColumnsMetaData> primaryMetas) {
        final String[] split = compositeKey.split(ExtConstants.PRIMARY_DELIMITER);
        StringBuffer conditionBuffer = new StringBuffer();
        if (split.length == primaryMetas.size()) {
            IntStream.range(0, primaryMetas.size()).forEach(idx -> {
                String condition = "";
                final ColumnsMetaData mate = primaryMetas.get(idx);
                if (idx > 0) {
                    condition = condition.concat(AND);
                }
                if (DIGITAL.contains(mate.getDataType())) {
                    condition = condition.concat(mate.getColumnName())
                            .concat(EQUAL)
                            .concat(split[idx]);
                } else {
                    condition = condition.concat(mate.getColumnName())
                            .concat(EQUAL)
                            .concat(SINGLE_QUOTES)
                            .concat(split[idx])
                            .concat(SINGLE_QUOTES);
                }
                conditionBuffer.append(condition);
            });
        }
        return conditionBuffer.toString();
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
