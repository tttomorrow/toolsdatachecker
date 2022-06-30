package org.opengauss.datachecker.common.entry.enums;

import lombok.Getter;


@Getter
public enum ColumnKey implements IEnum {
    /**
     * 主键
     */
    PRI("PRI"),
    /**
     * UNI
     */
    UNI("UNI"),
    /**
     * MUL
     */
    MUL("MUL");

    private final String code;
    private String description;

    ColumnKey(String code) {
        this.code = code;
    }

}
