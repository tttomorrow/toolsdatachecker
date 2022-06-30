package org.opengauss.datachecker.common.entry.enums;

import lombok.Getter;

/**
 * {@value API_DESCRIPTION}
 * @author ：wangchao
 * @date ：Created in 2022/6/12
 * @since ：11
 */
@Getter
public enum DML implements IEnum {
    /**
     * Insert插入语句
     */
    INSERT("INSERT", "InsertStatement"),
    /**
     * Delete删除语句
     */
    DELETE("DELETE", "DeleteStatement"),
    /**
     * Replace修改语句
     */
    REPLACE("REPLACE", "ReplaceStatement");

    private final String code;
    private final String description;

    DML(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public static final String API_DESCRIPTION = "DML [INSERT-InsertStatement,DELETE-DeleteStatement,REPLACE-ReplaceStatement]";
}
