package org.opengauss.datachecker.check.modules.check;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.modules.bucket.Bucket;
import org.opengauss.datachecker.check.modules.bucket.BuilderBucketHandler;
import org.opengauss.datachecker.check.modules.merkle.MerkleTree;
import org.opengauss.datachecker.check.modules.merkle.MerkleTree.Node;
import org.opengauss.datachecker.common.constant.Constants;
import org.opengauss.datachecker.common.entry.check.DataCheckParam;
import org.opengauss.datachecker.common.entry.check.DifferencePair;
import org.opengauss.datachecker.common.entry.check.Pair;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.exception.LargeDataDiffException;
import org.opengauss.datachecker.common.exception.MerkleTreeDepthException;
import org.springframework.lang.NonNull;
import org.springframework.util.CollectionUtils;

import java.util.*;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Slf4j
public class DataCheckThread implements Runnable {
    private static final int THRESHOLD_MIN_BUCKET_SIZE = 2;
    private static final String THREAD_NAME_PRIFEX = "data-check-";

    private final Topic topic;
    private final String tableName;
    private final int partitions;
    private final int bucketCapacity;
    private final String path;
    private final String sinkSchema;

    private final FeignClientService feignClient;

    private final List<Bucket> sourceBucketList = new ArrayList<>();
    private final List<Bucket> sinkBucketList = new ArrayList<>();
    private final DifferencePair<Map<String, RowDataHash>, Map<String, RowDataHash>, Map<String, Pair<Node, Node>>> difference
            = DifferencePair.of(new HashMap<>(), new HashMap<>(), new HashMap<>());

    private final Map<Integer, Pair<Integer, Integer>> bucketNumberDiffMap = new HashMap<>();
    private final QueryRowDataWapper queryRowDataWapper;
    private final DataCheckWapper dataCheckWapper;

    public DataCheckThread(@NonNull DataCheckParam checkParam, @NonNull FeignClientService feignClient) {
        this.topic = checkParam.getTopic();
        this.tableName = topic.getTableName();
        this.partitions = checkParam.getPartitions();
        this.path = checkParam.getPath();
        this.sinkSchema = checkParam.getSchema();
        this.bucketCapacity = checkParam.getBucketCapacity();
        this.feignClient = feignClient;
        this.queryRowDataWapper = new QueryRowDataWapper(feignClient);
        this.dataCheckWapper = new DataCheckWapper();
        resetThreadName();
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {

        // 初始化桶列表
        initBucketList();

        //不进行默克尔树校验算法场景
        if (checkNotMerkleCompare(sourceBucketList.size(), sinkBucketList.size())) {
            return;
        }

        // 构造默克尔树 约束: bucketList 不能为空，且size>=2
        MerkleTree sourceTree = new MerkleTree(sourceBucketList);
        MerkleTree sinkTree = new MerkleTree(sinkBucketList);

        // 默克尔树比较
        if (sourceTree.getDepth() != sinkTree.getDepth()) {
            throw new MerkleTreeDepthException(String.format("source & sink data have large different, Please synchronize data again! " +
                            "merkel tree depth different,source depth=[%d],sink depth=[%d]",
                    sourceTree.getDepth(), sinkTree.getDepth()));
        }
        //递归比较两颗默克尔树，并将差异记录返回。
        compareMerkleTree(sourceTree, sinkTree);
        // 校验结果  校验修复报告
        checkResult();
        cleanCheckThreadEnvironment();
    }

    private void cleanCheckThreadEnvironment() {
        bucketNumberDiffMap.clear();
        sourceBucketList.clear();
        sinkBucketList.clear();
        difference.getOnlyOnLeft().clear();
        difference.getOnlyOnRight().clear();
        difference.getDiffering().clear();
    }


    /**
     * 初始化桶列表
     */
    private void initBucketList() {
        // 获取当前任务对应kafka分区号
        // 初始化源端桶列列表数据
        initBucketList(Endpoint.SOURCE, partitions, sourceBucketList);
        // 初始化宿端桶列列表数据
        initBucketList(Endpoint.SINK, partitions, sinkBucketList);
        // 对齐源端宿端桶列表
        alignAllBuckets();
        // 排序
        sortBuckets(sourceBucketList);
        sortBuckets(sinkBucketList);
    }

    /**
     * 根据桶编号对最终桶列表进行排序
     *
     * @param bucketList 桶列表
     */
    private void sortBuckets(@NonNull List<Bucket> bucketList) {
        bucketList.sort(Comparator.comparingInt(Bucket::getNumber));
    }

    /**
     * 根据统计的源端宿端桶差异信息{@code bucketNumberDiffMap}结果，对齐桶列表数据。
     */
    private void alignAllBuckets() {
        dataCheckWapper.alignAllBuckets(bucketNumberDiffMap, sourceBucketList, sinkBucketList);
    }

    /**
     * 拉取指定端点{@code endpoint}服务当前表{@code tableName}的kafka分区{@code partitions}数据。
     * 并将kafka数据分组组装到指定的桶列表{@code bucketList}中
     *
     * @param endpoint   端点类型
     * @param partitions kafka分区号
     * @param bucketList 桶列表
     */
    private void initBucketList(Endpoint endpoint, int partitions, List<Bucket> bucketList) {
        Map<Integer, Bucket> bucketMap = new HashMap<>(Constants.InitialCapacity.MAP);
        // 使用feignclient 拉取kafka数据
        List<RowDataHash> dataList = getTopicPartitionsData(endpoint, partitions);
        if (CollectionUtils.isEmpty(dataList)) {
            return;
        }
        BuilderBucketHandler bucketBuilder = new BuilderBucketHandler(bucketCapacity);

        // 拉取的数据进行构建桶列表
        bucketBuilder.builder(dataList, dataList.size(), bucketMap);
        // 统计桶列表信息
        bucketNoStatistics(endpoint, bucketMap.keySet());
        bucketList.addAll(bucketMap.values());
    }

    /**
     * 比较两颗默克尔树，并将差异记录返回。
     *
     * @param sourceTree 源端默克尔树
     * @param sinkTree   宿端默克尔树
     */
    private void compareMerkleTree(@NonNull MerkleTree sourceTree, @NonNull MerkleTree sinkTree) {
        Node source = sourceTree.getRoot();
        Node sink = sinkTree.getRoot();
        List<Pair<Node, Node>> diffNodeList = new LinkedList<>();
        compareMerkleTree(source, sink, diffNodeList);
        if (CollectionUtils.isEmpty(diffNodeList)) {
            return;
        }
        diffNodeList.forEach(diffNode -> {
            Bucket sourceBucket = diffNode.getSource().getBucket();
            Bucket sinkBucket = diffNode.getSink().getBucket();
            DifferencePair<Map, Map, Map> subDifference = compareBucket(sourceBucket, sinkBucket);
            difference.getDiffering().putAll(subDifference.getDiffering());
            difference.getOnlyOnLeft().putAll(subDifference.getOnlyOnLeft());
            difference.getOnlyOnRight().putAll(subDifference.getOnlyOnRight());
        });

    }

    /**
     * 比较两个桶内部记录的差异数据
     * <p>
     * 差异类型 {@linkplain org.opengauss.datachecker.common.entry.enums.DiffCategory}
     *
     * @param sourceBucket 源端桶
     * @param sinkBucket   宿端桶
     * @return 差异记录
     */
    private DifferencePair<Map, Map, Map> compareBucket(Bucket sourceBucket, Bucket sinkBucket) {

        Map<String, RowDataHash> sourceMap = sourceBucket.getBucket();
        Map<String, RowDataHash> sinkMap = sinkBucket.getBucket();

        MapDifference<String, RowDataHash> difference = Maps.difference(sourceMap, sinkMap);

        Map<String, RowDataHash> entriesOnlyOnLeft = difference.entriesOnlyOnLeft();
        Map<String, RowDataHash> entriesOnlyOnRight = difference.entriesOnlyOnRight();
        Map<String, MapDifference.ValueDifference<RowDataHash>> entriesDiffering = difference.entriesDiffering();
        Map<String, Pair<RowDataHash, RowDataHash>> differing = new HashMap<>(Constants.InitialCapacity.MAP);
        entriesDiffering.forEach((key, diff) -> {
            differing.put(key, Pair.of(diff.leftValue(), diff.rightValue()));
        });
        return DifferencePair.of(entriesOnlyOnLeft, entriesOnlyOnRight, differing);
    }

    /**
     * 递归比较两颗默克尔树节点，并记录差异节点。
     * <p>
     * 采用递归-前序遍历方式，遍历比较默克尔树，从而查找差异节点。
     * <p>
     * 若当前遍历的节点{@link org.opengauss.datachecker.check.modules.merkle.MerkleTree.Node}签名相同则终止当前遍历分支。
     *
     * @param source       源端默克尔树节点
     * @param sink         宿端默克尔树节点
     * @param diffNodeList 差异节点记录
     */
    private void compareMerkleTree(@NonNull Node source, @NonNull Node sink, List<Pair<Node, Node>> diffNodeList) {
        // 如果节点相同，则退出
        if (Arrays.equals(source.getSignature(), sink.getSignature())) {
            return;
        }
        // 如果节点不相同，则继续比较下层节点，若当前差异节点为叶子节点，则记录该差异节点，并退出
        if (source.getType() == MerkleTree.LEAF_SIG_TYPE) {
            diffNodeList.add(Pair.of(source, sink));
            return;
        }
        compareMerkleTree(source.getLeft(), sink.getLeft(), diffNodeList);

        compareMerkleTree(source.getRight(), sink.getRight(), diffNodeList);

    }

    /**
     * 对各端点构建的桶编号进行统计。统计结果汇总到{@code bucketNumberDiffMap}中。
     * <p>
     * 默克尔比较算法，需要确保双方桶编号的一致。
     * <p>
     * 如果一方的桶编号存在缺失，即{@code Pair<S,T>}中，S或T的值为-1，则需要生成相应编号的空桶。
     *
     * @param endpoint    端点
     * @param bucketNoSet 桶编号
     */
    private void bucketNoStatistics(@NonNull Endpoint endpoint, @NonNull Set<Integer> bucketNoSet) {
        bucketNoSet.forEach(bucketNo -> {
            if (!bucketNumberDiffMap.containsKey(bucketNo)) {
                if (endpoint == Endpoint.SOURCE) {
                    bucketNumberDiffMap.put(bucketNo, Pair.of(bucketNo, -1));
                } else {
                    bucketNumberDiffMap.put(bucketNo, Pair.of(-1, bucketNo));
                }
            } else {
                Pair<Integer, Integer> pair = bucketNumberDiffMap.get(bucketNo);
                if (endpoint == Endpoint.SOURCE) {
                    bucketNumberDiffMap.put(bucketNo, Pair.of(bucketNo, pair));
                } else {
                    bucketNumberDiffMap.put(bucketNo, Pair.of(pair, bucketNo));
                }
            }
        });
    }

    /**
     * 拉取指定端点{@code endpoint}的表{@code tableName}的 kafka分区{@code partitions}数据
     *
     * @param endpoint   端点类型
     * @param partitions kafka分区号
     * @return 指定表 kafka分区数据
     */
    private List<RowDataHash> getTopicPartitionsData(Endpoint endpoint, int partitions) {
        return queryRowDataWapper.queryRowData(endpoint, tableName, partitions);
    }

    /**
     * 不满足默克尔树约束条件下 比较
     *
     * @param sourceBucketCount 源端数量
     * @param sinkBucketCount   宿端数量
     * @return 是否满足默克尔校验场景
     */
    private boolean checkNotMerkleCompare(int sourceBucketCount, int sinkBucketCount) {
        // 满足构造默克尔树约束条件
        if (sourceBucketCount >= THRESHOLD_MIN_BUCKET_SIZE && sinkBucketCount >= THRESHOLD_MIN_BUCKET_SIZE) {
            return false;
        }
        // 不满足默克尔树约束条件下 比较
        if (sourceBucketCount == sinkBucketCount) {
            // sourceSize等于0，即都是空桶
            if (sourceBucketCount == 0) {
                //表是空表, 校验成功!
                log.info("table[{}] is an empty table,this check successful!", tableName);
            } else {
                // sourceSize小于thresholdMinBucketSize 即都只有一个桶，比较
                DifferencePair<Map, Map, Map> subDifference = compareBucket(sourceBucketList.get(0), sinkBucketList.get(0));
                difference.getDiffering().putAll(subDifference.getDiffering());
                difference.getOnlyOnLeft().putAll(subDifference.getOnlyOnLeft());
                difference.getOnlyOnRight().putAll(subDifference.getOnlyOnRight());
            }
        } else {
            throw new LargeDataDiffException(String.format("table[%s] source & sink data have large different," +
                    "source-bucket-count=[%s] sink-bucket-count=[%s]" +
                    " Please synchronize data again! ", tableName, sourceBucketCount, sinkBucketCount));
        }
        return true;
    }

    private void checkResult() {
        CheckDiffResult result = AbstractCheckDiffResultBuilder.builder(feignClient)
                .table(tableName)
                .topic(topic.getTopicName())
                .schema(sinkSchema)
                .partitions(partitions)
                .keyUpdateSet(difference.getDiffering().keySet())
                .keyInsertSet(difference.getOnlyOnLeft().keySet())
                .keyDeleteSet(difference.getOnlyOnRight().keySet())
                .build();
        ExportCheckResult.export(path, result);
    }

    /**
     * 重置当前线程 线程名称
     */
    private void resetThreadName() {
        Thread.currentThread().setName(THREAD_NAME_PRIFEX + topic.getTopicName());
    }
}
