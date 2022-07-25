package org.opengauss.datachecker.common.entry.enums;

import lombok.Getter;

/**
 * {@value API_DESCRIPTION}
 */
@Getter
public enum DataBaseType implements IEnum {
    /**
     * MySQL数据库类型
     */
    MS("MYSQL", "MYSQL"),
    /**
     * open gauss数据库
     */
    OG("OPENGAUSS", "OPENGAUSS"),
    /**
     * oracle数据库
     */
    O("ORACLE", "ORACLE");

    private final String code;
    private final String description;

    DataBaseType(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public static final String API_DESCRIPTION = "Database type [MS-MYSQL,OG-OPENGAUSS,O-ORACLE]";
}
