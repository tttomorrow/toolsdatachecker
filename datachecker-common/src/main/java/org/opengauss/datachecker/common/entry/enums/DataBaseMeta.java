package org.opengauss.datachecker.common.entry.enums;

import lombok.Getter;

@Getter
public enum DataBaseMeta implements IEnum {
    /**
     * TableMetaData
     */
    TABLE("TableMetaData", "TableMetaData"),
    /**
     * TablesColumnMetaData
     */
    COLUMN("TablesColumnMetaData", "TablesColumnMetaData");

    private final String code;
    private final String description;

    DataBaseMeta(String code, String description) {
        this.code = code;
        this.description = description;
    }

}
