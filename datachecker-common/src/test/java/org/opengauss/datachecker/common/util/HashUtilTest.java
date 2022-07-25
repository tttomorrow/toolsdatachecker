package org.opengauss.datachecker.common.util;

import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;


class HashUtilTest {

    @Test
    void testHashBytes1() {
        int mod = 600;
        AtomicInteger min = new AtomicInteger();
        AtomicInteger max = new AtomicInteger();
        int[] resultCount = new int[mod * 2];
        IntStream.range(1, 100000).forEach(idx -> {
            long x = HashUtil.hashBytes(UUID.randomUUID().toString().getBytes());
            int xmod = (int) (x % mod + mod);
            max.set(Math.max(xmod, max.get()));
            min.set(Math.min(xmod, min.get()));
            resultCount[xmod]++;
            //System.out.println(x + "  " + xmod);
        });
        System.out.println("min=" + min.get() + "   max=" + max.get());
        IntStream.range(0, mod * 2).forEach(idx -> {
            System.out.println("idx=" + idx + "  " + resultCount[idx]);
        });

    }

    @Test
    void testHashBytes2() {
        System.out.println("bucket average capacity  " + 100000 / 1200);
        System.out.println("merkle leaf node size  " + Math.pow(2, 15));
        System.out.println("merkle leaf node size  " + (1 << 15));
    }

    @Test
    void testMode() {
        int mod = (int) Math.pow(2, 3);
        long x = HashUtil.hashBytes(UUID.randomUUID().toString().getBytes());
        int xmod = (int) (x % mod + mod);
        System.out.println(" mod=  " + mod);
        System.out.println(x + "   " + xmod);

        System.out.println(x + "  (int) (x % mod ) =" + (int) (x % mod));
        System.out.println(x + " ( x & (2^n - 1) )= " + (x & (mod - 1)));

        System.out.println(x + "  (int) (x % mod + mod) =" + (int) (x % mod + mod));
        System.out.println(x + " ( x & (2^n - 1) + 2^n )= " + ((x & (mod - 1)) + mod));


    }

    private static final int[] SCOP_BUCKET_COUNT = new int[15];

    static {
        SCOP_BUCKET_COUNT[0] = 1 << 1;
        SCOP_BUCKET_COUNT[1] = 1 << 2;
        SCOP_BUCKET_COUNT[2] = 1 << 3;
        SCOP_BUCKET_COUNT[3] = 1 << 4;
        SCOP_BUCKET_COUNT[4] = 1 << 5;
        SCOP_BUCKET_COUNT[5] = 1 << 6;
        SCOP_BUCKET_COUNT[6] = 1 << 7;
        SCOP_BUCKET_COUNT[7] = 1 << 8;
        SCOP_BUCKET_COUNT[8] = 1 << 9;
        SCOP_BUCKET_COUNT[9] = 1 << 10;
        SCOP_BUCKET_COUNT[10] = 1 << 11;
        SCOP_BUCKET_COUNT[11] = 1 << 12;
        SCOP_BUCKET_COUNT[12] = 1 << 13;
        SCOP_BUCKET_COUNT[13] = 1 << 14;
        SCOP_BUCKET_COUNT[14] = 1 << 15;
    }

    @Test
    public void calacBucketCount() {
        int totalCount = 5;
        int bucketCount = totalCount / 5;
        System.out.println(bucketCount);
        int asInt = IntStream.range(0, 15)
                .filter(idx -> SCOP_BUCKET_COUNT[idx] > bucketCount)
                .peek(System.out::println)
                .findFirst()
                .orElse(15);
        System.out.println(SCOP_BUCKET_COUNT[asInt]);

    }

}
