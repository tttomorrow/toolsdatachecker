package org.opengauss.datachecker.common.util;

import org.opengauss.datachecker.common.entry.enums.IEnum;

public class EnumUtil {

    /**
     * 获取枚举
     *
     * @param clazz
     * @param code
     * @return
     */
    public static <T extends IEnum> T valueOfIgnoreCase(Class<T> clazz, String code) {
        return valueOf(clazz, code, true);
    }

    /**
     * 获取枚举,区分大小写
     *
     * @param clazz
     * @param code
     * @param isIgnore
     * @return
     */
    public static <T extends IEnum> T valueOf(Class<T> clazz, String code, boolean isIgnore) {

        //得到values
        T[] enums = values(clazz);

        if (enums == null || enums.length == 0) {
            return null;
        }

        for (T t : enums) {
            if (isIgnore && t.getCode().equalsIgnoreCase(code)) {
                return t;
            } else if (t.getCode().equals(code)) {
                return t;
            }
        }
        return null;
    }
    /**
     * 获取枚举,区分大小写
     *
     * @param clazz
     * @param code
     * @return
     */
    public static <T extends IEnum> T valueOf(Class<T> clazz, String code) {
        return valueOf(clazz, code, false);
    }

    /**
     * 获取枚举集合
     *
     * @param clazz
     * @return
     */
    public static <T extends IEnum> T[] values(Class<T> clazz) {
        if (!clazz.isEnum()) {
            throw new IllegalArgumentException("Class[" + clazz + "]不是枚举类型");
        }
        //得到values
        return clazz.getEnumConstants();
    }

}
