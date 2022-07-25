package org.opengauss.datachecker.extract.dml;

import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/14
 * @since ：11
 */
public class InsertDmlBuilder extends DmlBuilder {


    /**
     * 构建 Schema
     *
     * @param schema Schema
     * @return InsertDMLBuilder 构建器
     */
    public InsertDmlBuilder schema(@NotNull String schema) {
        super.buildSchema(schema);
        return this;
    }

    /**
     * 构建 tableName
     *
     * @param tableName tableName
     * @return InsertDMLBuilder 构建器
     */
    public InsertDmlBuilder tableName(@NotNull String tableName) {
        super.buildTableName(tableName);
        return this;
    }

    /**
     * 构建SQL column 语句片段
     *
     * @param columnsMetas 字段元数据
     * @return InsertDMLBuilder 构建器
     */
    public InsertDmlBuilder columns(@NotNull List<ColumnsMetaData> columnsMetas) {
        this.columns = columnsMetas.stream()
                .map(ColumnsMetaData::getColumnName)
                .collect(Collectors.joining(DELIMITER));
        return this;
    }

    /**
     * 构建SQL column value 语句片段
     *
     * @param columnsMetaList 字段元数据
     * @return InsertDMLBuilder 构建器
     */
    public InsertDmlBuilder columnsValue(@NotNull Map<String, String> columnsValue, @NotNull List<ColumnsMetaData> columnsMetaList) {
        List<String> valueList = new ArrayList<>(columnsValueList(columnsValue, columnsMetaList));
        this.columnsValue = String.join(DELIMITER, valueList);
        return this;
    }

    public String build() {
        return Fragment.DML_INSERT.replace(Fragment.SCHEMA, schema)
                .replace(Fragment.TABLE_NAME, tableName)
                .replace(Fragment.COLUMNS, columns)
                .replace(Fragment.VALUE, columnsValue)
                ;
    }

}
