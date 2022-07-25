package org.opengauss.datachecker.extract.dml;


import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.extract.constants.ExtConstants;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/13
 * @since ：11
 */
public class SelectDmlBuilder extends DmlBuilder {

    /**
     * 构建SQL column 语句片段
     *
     * @param columnsMetas 字段元数据
     * @return SelectDMLBuilder构建器
     */
    public SelectDmlBuilder columns(@NotNull List<ColumnsMetaData> columnsMetas) {
        super.buildColumns(columnsMetas);
        return this;
    }

    /**
     * 构建 Schema
     *
     * @param schema Schema
     * @return SelectDMLBuilder构建器
     */
    public SelectDmlBuilder schema(@NotNull String schema) {
        super.buildSchema(schema);
        return this;
    }

    /**
     * 生成单一主键字段 select columns... from where pk in (参数...) 条件语句
     *
     * @param primaryMeta 主键元数据
     * @return SelectDMLBuilder构建器
     */
    public SelectDmlBuilder conditionPrimary(@NonNull ColumnsMetaData primaryMeta) {
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
    public SelectDmlBuilder conditionCompositePrimary(@NonNull List<ColumnsMetaData> primaryMeta) {
        this.condition = buildConditionCompositePrimary(primaryMeta).concat(IN);
        return this;
    }



    /**
     * 构建复合主键参数的条件查询语句 value 参数<p>
     * select columns... from table where (pk1,pk2) in ((pk1_val,pk2_val),(pk1_val,pk2_val))<p>
     *
     * @param primaryMetas  主键元数据信息
     * @param compositeKeys 主键值列表
     * @return SelectDMLBuilder构建器
     */
    public List<Object[]> conditionCompositePrimaryValue(@NonNull List<ColumnsMetaData> primaryMetas, List<String> compositeKeys) {
        List<Object[]> batchParam = new ArrayList<>();
        final int size = primaryMetas.size();
        compositeKeys.forEach(compositeKey -> {
            final String[] split = compositeKey.split(ExtConstants.PRIMARY_DELIMITER);
            if (split.length == size) {
                Object[] values = new Object[size];
                IntStream.range(0, primaryMetas.size()).forEach(idx -> {
                    values[idx] = split[idx];
                });
                batchParam.add(values);
            }
        });
        return batchParam;
    }

    /**
     * 构建 tableName
     *
     * @param tableName tableName
     * @return SelectDMLBuilder构建器
     */
    public SelectDmlBuilder tableName(@NotNull String tableName) {
        super.buildTableName(tableName);
        return this;
    }


    public String build() {
        StringBuffer sb = new StringBuffer();
        sb.append(Fragment.SELECT).append(columns).append(Fragment.FROM)
                .append(schema).append(Fragment.LINKER).append(tableName)
                .append(Fragment.WHERE).append(condition)
                .append(Fragment.END)
        ;
        return sb.toString();
    }

}
