package org.opengauss.datachecker.common.entry.enums;

public interface IEnum {
    /**
     * 定义枚举code
     *
     * @return 返回枚举code
     */
    String getCode();

    /**
     * 声明枚举描述
     *
     * @return 返回枚举描述
     */
    String getDescription();
}
