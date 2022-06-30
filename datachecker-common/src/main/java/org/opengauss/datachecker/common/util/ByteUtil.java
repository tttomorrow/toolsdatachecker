package org.opengauss.datachecker.common.util;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class ByteUtil {
   
    /**
     * 比较两个字节数组是否一致
     *
     * @param byte1 字节数组
     * @param byte2 字节数组
     * @return true|false
     */
    public static boolean isEqual(byte[] byte1, byte[] byte2) {
        if (byte1 == null || byte2 == null || byte1.length != byte2.length) {
            return false;
        }
        return HashUtil.hashBytes(byte1) == HashUtil.hashBytes(byte2);
    }

    /**
     * 将long型数字转化为byte字节数组
     *
     * @param value long型数组
     * @return 字节数组
     */
    public static byte[] toBytes(long value) {
        return new byte[]{
                (byte) (value >> 56),
                (byte) (value >> 48),
                (byte) (value >> 40),
                (byte) (value >> 32),
                (byte) (value >> 24),
                (byte) (value >> 16),
                (byte) (value >> 8),
                (byte) value
        };
    }
}
