package org.opengauss.datachecker.common.entry.enums;

import lombok.Getter;

/**
 * {@value API_DESCRIPTION }
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
@Getter
public enum CheckBlackWhiteMode implements IEnum {
    /**
     * 不开启黑白名单模式
     */
    NONE("NONE", "do not turn on black and white list mode"),
    /**
     * 黑名单校验
     */
    BLACK("BLACK", "blacklist verification mode"),
    /**
     * 白名单校验
     */
    WHITE("WHITE", "white list verification mode");

    private final String code;
    private final String description;

    CheckBlackWhiteMode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public static final String API_DESCRIPTION = "black and white list verification mode [" +
            " NONE-do not turn on black and white list mode," +
            " BLACK-blacklist verification mode," +
            " WHITE-white list verification mode" +
            "]";
}
