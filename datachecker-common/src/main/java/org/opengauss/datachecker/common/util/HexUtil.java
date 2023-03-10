package org.opengauss.datachecker.common.util;

/**
 * @author ：wangchao
 * @date ：Created in 2023/3/10
 * @since ：11
 */
public class HexUtil {
    private static final char[] CHARS = "0123456789ABCDEF".toCharArray();

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
        StringBuilder result = new StringBuilder("\\x");
        for (byte datum : data) {
            result.append(Integer.toHexString((datum & 0xFF) | 0x100).toUpperCase().substring(1, 3));
        }
        return result.toString();
    }

    /**
     * Clear the 0 at the end of the byte array and convert valid values to hexadecimal strings
     *
     * @param data data
     * @return
     */
    public static String byteToHexTrim(byte[] data) {
        StringBuilder result = new StringBuilder("\\x");
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
