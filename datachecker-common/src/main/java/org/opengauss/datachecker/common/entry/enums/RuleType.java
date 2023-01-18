package org.opengauss.datachecker.common.entry.enums;

import lombok.Getter;

/**
 * RuleType
 *
 * @author ：wangchao
 * @date ：Created in 2022/11/30
 * @since ：11
 */
@Getter
public enum RuleType implements IEnum {
    /**
     * table
     */
    TABLE("table"),
    /**
     * Sink
     */
    ROW("row"),
    /**
     * Sink
     */
    COLUMN("column");

    private final String code;
    private String description;

    RuleType(String code) {
        this.code = code;
    }

    /**
     * DataSourceType api description
     */
    public static final String API_DESCRIPTION = "RuleType [TABLE ,ROW, COLUMN]";

}
