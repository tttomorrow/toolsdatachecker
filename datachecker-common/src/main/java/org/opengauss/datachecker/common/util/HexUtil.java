package org.opengauss.datachecker.common.util;

/**
 * @author ：wangchao
 * @date ：Created in 2023/3/10
 * @since ：11
 */
public class HexUtil {
    private static final char[] CHARS = "0123456789ABCDEF".toCharArray();
    public static final String HEX_ZERO_PREFIX = "0x";
    public static final String HEX_PREFIX = "\\x";
    private static final String HEX_NO_PREFIX = "";

    /**
     * Convert text string to hexadecimal string
     *
     * @param str str
     * @return
     */
    public static String toHex(String str) {
        StringBuilder sb = new StringBuilder("");
        byte[] bs = str.getBytes();
        int bit;
        for (int i = 0; i < bs.length; i++) {
            bit = (bs[i] & 0x0f0) >> 4;
            sb.append(CHARS[bit]);
            bit = bs[i] & 0x0f;
            sb.append(CHARS[bit]);
        }
        return sb.toString().trim();
    }

    /**
     * Convert byte array to hexadecimal string
     *
     * @param data data
     * @return
     */
    public static String byteToHex(byte[] data) {
        StringBuilder result = new StringBuilder();
        for (byte datum : data) {
            result.append(Integer.toHexString((datum & 0xFF) | 0x100).toUpperCase().substring(1, 3));
        }
        return result.toString();
    }

    /**
     * Clear the 0 at the end of the byte array and convert valid values to hexadecimal strings
     * 02AA -> 0x02AA
     *
     * @param data data
     * @return
     */
    public static String byteToHexTrimBackslash(byte[] data) {
        return byteToHexTrim(data, HEX_PREFIX);
    }

    /**
     * Clear the 0 at the end of the byte array and convert valid values to hexadecimal strings
     * 02AA -> 02AA
     *
     * @param data data
     * @return
     */
    public static String byteToHexTrim(byte[] data) {
        return byteToHexTrim(data, HEX_NO_PREFIX);
    }

    /**
     * Clear the 0 at the end of the byte array and convert valid values to hexadecimal strings
     * 02AA -> 0x02AA
     *
     * @param data data
     * @return
     */
    public static String byteToHexTrimZero(byte[] data) {
        return byteToHexTrim(data, HEX_ZERO_PREFIX);
    }

    private static String byteToHexTrim(byte[] data, String prefix) {
        StringBuilder result = new StringBuilder(prefix);
        int fast = 0;
        int slow = 0;
        final int end = data.length;
        while (fast < end) {
            if (data[fast] != 0) {
                while (slow < fast) {
                    result.append(Integer.toHexString((data[slow++] & 0xFF) | 0x100).toUpperCase().substring(1, 3));
                }
                slow = fast;
            }
            fast++;
        }
        result.append(Integer.toHexString((data[slow] & 0xFF) | 0x100).toUpperCase().substring(1, 3));
        return result.toString();
    }
}
