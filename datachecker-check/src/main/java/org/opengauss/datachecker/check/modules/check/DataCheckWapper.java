package org.opengauss.datachecker.check.modules.check;

import org.opengauss.datachecker.check.modules.bucket.Bucket;
import org.opengauss.datachecker.check.modules.bucket.BuilderBucketHandler;
import org.opengauss.datachecker.common.entry.check.Pair;
import org.springframework.lang.NonNull;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/18
 * @since ：11
 */
public class DataCheckWapper {


    /**
     * 根据统计的源端宿端桶差异信息{@code bucketNumberDiffMap}结果，对齐桶列表数据。
     *
     * @param bucketNumberDiffMap 源端宿端桶差异信息
     * @param sourceBucketList    源端桶列表
     * @param sinkBucketList      宿端通列表
     */
    public void alignAllBuckets(Map<Integer, Pair<Integer, Integer>> bucketNumberDiffMap,
                                @NonNull List<Bucket> sourceBucketList, @NonNull List<Bucket> sinkBucketList) {
        if (!CollectionUtils.isEmpty(bucketNumberDiffMap)) {
            bucketNumberDiffMap.forEach((number, pair) -> {
                if (pair.getSource() == -1) {
                    sourceBucketList.add(BuilderBucketHandler.builderEmpty(number));
                }
                if (pair.getSink() == -1) {
                    sinkBucketList.add(BuilderBucketHandler.builderEmpty(number));
                }
            });
        }
    }
}
