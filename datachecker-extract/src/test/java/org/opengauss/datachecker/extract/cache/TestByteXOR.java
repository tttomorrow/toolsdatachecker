package org.opengauss.datachecker.extract.cache;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/14
 * @since ：11
 */
public class TestByteXOR {

    @Test
    public void testXOR() {
        long old = 0L;
        for (int i = 0; i < 63; i++) {
            old = byteXor(old, i);
            System.out.println("0 ," + i + " =" + old + " Long.toBinaryString()" + Long.toBinaryString(old));
        }
    }

    @Test
    public void testBinaryArray() {
        Map<String, Byte[]> map = new HashMap<String, Byte[]>();
        long old = 0L;
        byte byteVal = 0;
        for (int i = 0; i < 63; i++) {
            old = byteXor(old, i);
            System.out.println("0 ," + i + " =" + old + " Long.toBinaryString()" + Long.toBinaryString(old));
        }
    }

    @Test
    public void testIntStream() {
        IntStream.range(1, 10).forEach(idx -> {
            System.out.println("range " + idx);
        });
        IntStream.rangeClosed(1, 10).forEach(idx -> {
            System.out.println("rangeClosed " + idx);
        });

        IntStream.rangeClosed(1, 10)
                .filter(i -> i == 6)
                .count()
        ;
    }

    /**
     * long 64为 该方法计算后63标识符保存数据状态。
     *
     * @param value
     * @param index
     * @return
     */
    long byteXor(long value, int index) {
        return (value | (1L << index));
    }
}
