package org.opengauss.datachecker.check.modules.bucket;

import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.springframework.lang.NonNull;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/24
 * @since ：11
 */
public class BuilderBucketHandler {
    /**
     * 默克尔树最大树高度
     */
    private static final int MERKLE_TREE_MAX_HEIGHT = 15;
    /**
     * 最高默克尔树的最大叶子节点数量
     */
    private static final int BUCKET_MAX_COUNT_LIMITS = 1 << MERKLE_TREE_MAX_HEIGHT;


    /**
     * 当限定了默克尔树最大树高度为{@value MERKLE_TREE_MAX_HEIGHT}，
     * 那么构造的最高默克尔树的最大叶子节点数量为{@code BUCKET_MAX_COUNT_LIMITS} 即 {@value BUCKET_MAX_COUNT_LIMITS}。
     * <p>
     * 由此，获得最大桶数量值为{@value BUCKET_MAX_COUNT_LIMITS }，
     * 桶数量范围我们限定每棵树桶的数量为 2^n 个
     */
    private static final int[] BUCKET_COUNT_LIMITS = new int[MERKLE_TREE_MAX_HEIGHT];

    // 初始化{@code BUCKET_COUNT_LIMITS}
    static {
        for (int i = 1; i <= MERKLE_TREE_MAX_HEIGHT; i++) {
            BUCKET_COUNT_LIMITS[i - 1] = 1 << i;
        }
    }

    /**
     * 空桶容量大小，用于构造特殊的空桶
     */
    private static final int EMPTY_INITIAL_CAPACITY = 0;

    /**
     * 当前桶初始化容量
     */
    private final int bucketCapacity;

    public BuilderBucketHandler(int bucketCapacity) {
        this.bucketCapacity = bucketCapacity;
    }

    /**
     * 将{@code rowDataHashList}数据动态分配到桶{@link org.opengauss.datachecker.check.modules.bucket.Bucket}中。
     * <p>
     *
     * @param rowDataHashList 当前待分配到桶中的记录集合
     * @param totalCount      为{@link org.opengauss.datachecker.common.entry.extract.RowDataHash} 记录总数。
     *                        注意：不一定为当前{@code rowDataHashList.size}总数
     * @param bucketMap       {@code bucketMap<K,V>} K为当前桶V的编号。
     */
    public void builder(@NonNull List<RowDataHash> rowDataHashList, int totalCount, @NonNull Map<Integer, Bucket> bucketMap) {
        // 根据当前记录总数计算当前最大桶数量
        int maxBucketCount = calacMaxBucketCount(totalCount);
        // 桶平均容量-用于初始化桶容量大小
        int averageCapacity = totalCount / maxBucketCount;
        rowDataHashList.forEach(row -> {
            long primaryKeyHash = row.getPrimaryKeyHash();
            // 计算桶编号信息
            int bucketNumber = calacBucketNumber(primaryKeyHash, maxBucketCount);
            Bucket bucket;
            // 根据row 信息获取指定编号的桶，如果不存在则创建桶
            if (bucketMap.containsKey(bucketNumber)) {
                bucket = bucketMap.get(bucketNumber);
            } else {
                bucket = new Bucket(averageCapacity).setNumber(bucketNumber);
                bucketMap.put(bucketNumber, bucket);
            }
            // 将row 添加到指定桶编号的桶中
            bucket.put(row);
        });

    }

    /**
     * 根据{@code totalCount}记录总数计算当前最大桶数量。桶的数量为2^n个
     *
     * @param totalCount 记录总数
     * @return 最大桶数量
     */
    private int calacMaxBucketCount(int totalCount) {
        int bucketCount = totalCount / bucketCapacity;
        int asInt = IntStream.range(0, 15)
                .filter(idx -> BUCKET_COUNT_LIMITS[idx] > bucketCount)
                .findFirst()
                .orElse(15);
        return BUCKET_COUNT_LIMITS[asInt];
    }

    /**
     * 根据{@code rowHash}值对当前记录进行标记，此标记用于桶的编号
     *
     * @param primaryKeyHash 行记录主键哈希值
     * @param bucketCount    桶数量 桶的数量为2^n个
     * @return 行记录桶编号
     */
    private int calacBucketNumber(long primaryKeyHash, int bucketCount) {
        return (int) (Math.abs(primaryKeyHash) & (bucketCount - 1));
    }


    /**
     * 根据编号构造空桶
     *
     * @param bucketNumber 桶编号
     * @return 桶
     */
    public static Bucket builderEmpty(Integer bucketNumber) {
        return new Bucket(EMPTY_INITIAL_CAPACITY).setNumber(bucketNumber);
    }
}
