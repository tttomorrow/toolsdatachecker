package org.opengauss.datachecker.common.entry.enums;

import lombok.Getter;

@Getter
public enum DataSourceType implements IEnum {
    /**
     * 源端
     */
    Source("Source"),
    /**
     * 宿端
     */
    Sink("Sink");

    private final String code;
    private String description;

    DataSourceType(String code) {
        this.code = code;
    }

}
