package org.opengauss.datachecker.extract.dml;

import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/13
 * @since ：11
 */
public class DmlBuilder {

    protected static final String DELIMITER = ",";
    protected static final String LEFT_BRACKET = "(";
    protected static final String RIGHT_BRACKET = ")";
    protected static final String IN = " in ( :primaryKeys )";
    protected static final String SINGLE_QUOTES = "'";
    protected static final String EQUAL = " = ";
    protected static final String AND = " and ";
    public static final String PRIMARY_KEYS = "primaryKeys";
    /**
     * mysql dataType
     */
    protected final List<String> DIGITAL = List.of("int", "tinyint", "smallint", "mediumint", "bit", "bigint", "double", "float", "decimal");


    protected String columns;
    protected String columnsValue;
    protected String schema;
    protected String tableName;
    protected String condition;
    protected String conditionValue;


    /**
     * 构建SQL column 语句片段
     *
     * @param columnsMetas 字段元数据
     * @return SQL column 语句片段
     */
    protected void buildColumns(@NotNull List<ColumnsMetaData> columnsMetas) {
        this.columns = columnsMetas.stream()
                .map(ColumnsMetaData::getColumnName)
                .collect(Collectors.joining(DELIMITER));
    }

    protected void buildSchema(@NotNull String schema) {
        this.schema = schema;
    }

    protected void buildTableName(@NotNull String tableName) {
        this.tableName = tableName;
    }

    protected String buildConditionCompositePrimary(List<ColumnsMetaData> primaryMetas) {
        return primaryMetas.stream()
                .map(ColumnsMetaData::getColumnName)
                .collect(Collectors.joining(DELIMITER, LEFT_BRACKET, RIGHT_BRACKET));
    }

    public List<String> columnsValueList(@NotNull Map<String, String> columnsValue, @NotNull List<ColumnsMetaData> columnsMetaList) {
        List<String> valueList = new ArrayList<>();
        columnsMetaList.forEach(columnMeta -> {
            if (DIGITAL.contains(columnMeta.getDataType())) {
                valueList.add(columnsValue.get(columnMeta.getColumnName()));
            } else {
                String value = columnsValue.get(columnMeta.getColumnName());
                if (Objects.isNull(value)) {
                    valueList.add("null");
                } else {
                    valueList.add(SINGLE_QUOTES.concat(value).concat(SINGLE_QUOTES));
                }
            }
        });
        return valueList;
    }

    interface Fragment {
        String DML_INSERT = "insert into #schema.#tablename (#columns) value (#value);";
        String DML_REPLACE = "replace into #schema.#tablename (#columns) value (#value);";
        String SELECT = "select ";
        String DELETE = "delete ";
        String FROM = " from ";
        String WHERE = " where ";
        String SPACE = " ";
        String END = ";";
        String LINKER = ".";

        String SCHEMA = "#schema";
        String TABLE_NAME = "#tablename";
        String COLUMNS = "#columns";
        String VALUE = "#value";
    }
}
