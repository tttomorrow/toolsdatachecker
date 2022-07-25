package org.opengauss.datachecker.common.entry.enums;

import lombok.Getter;

/**
 * 校验方式
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
@Getter
public enum CheckMode implements IEnum {
    /**
     * 全量校验
     */
    FULL("FULL", "full check mode"),
    /**
     * 增量校验
     */
    INCREMENT("INCREMENT", "increment check mode");

    private final String code;
    private final String description;

    CheckMode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public static final String API_DESCRIPTION = "CheckMode [FULL-full check mode,INCREMENT-increment check mode]";
}
