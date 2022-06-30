package org.opengauss.datachecker.common.util;

import net.openhft.hashing.LongHashFunction;
import org.springframework.lang.NonNull;

import java.nio.charset.Charset;


/**
 * @author ：wangchao
 * @date ：Created in 2022/5/24
 * @since ：11
 */
public class HashUtil {

    /**
     * 哈希算法
     */
    private static final LongHashFunction XX_3_HASH = LongHashFunction.xx3();

    /**
     * 使用xx3哈希算法 对字符串进行哈希计算
     *
     * @param input 字符串
     * @return 哈希值
     */
    public static long hashChars(@NonNull String input) {
        return XX_3_HASH.hashChars(input);
    }
    /**
     * 使用xx3哈希算法 对字节数组进行哈希计算
     *
     * @param input 字节数组
     * @return 哈希值
     */
    public static long hashBytes(@NonNull byte[] input) {
        return XX_3_HASH.hashBytes(input);
    }

    /**
     * 使用xx3哈希算法 对字符串进行哈希计算
     *
     * @param input 字符串
     * @return 哈希值
     */
    public static long hashBytes(@NonNull String input) {
        return XX_3_HASH.hashBytes(input.getBytes(Charset.defaultCharset()));
    }
}
