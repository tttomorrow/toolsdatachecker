package org.opengauss.datachecker.check.modules.check;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.modules.bucket.Bucket;
import org.opengauss.datachecker.check.modules.bucket.BuilderBucketHandler;
import org.opengauss.datachecker.check.modules.merkle.MerkleTree;
import org.opengauss.datachecker.check.modules.merkle.MerkleTree.Node;
import org.opengauss.datachecker.common.entry.check.DataCheckParam;
import org.opengauss.datachecker.common.entry.check.DifferencePair;
import org.opengauss.datachecker.common.entry.check.Pair;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadataHash;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.exception.DispatchClientException;
import org.opengauss.datachecker.common.exception.MerkleTreeDepthException;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.lang.NonNull;
import org.springframework.util.CollectionUtils;

import java.util.*;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Slf4j
public class IncrementDataCheckThread implements Runnable {
    private static final int THRESHOLD_MIN_BUCKET_SIZE = 2;
    private static final String THREAD_NAME_PRIFEX = "increment-data-check-";

    private final Topic topic;
    private final String tableName;
    private final int partitions;
    private final int bucketCapacity;
    private final String path;

    private final FeignClientService feignClient;

    private final List<Bucket> sourceBucketList = new ArrayList<>();
    private final List<Bucket> sinkBucketList = new ArrayList<>();
    private final DifferencePair<Map<String, RowDataHash>, Map<String, RowDataHash>, Map<String, Pair<Node, Node>>> difference
            = DifferencePair.of(new HashMap<>(), new HashMap<>(), new HashMap<>());

    private final Map<Integer, Pair<Integer, Integer>> bucketNumberDiffMap = new HashMap<>();
    private final QueryRowDataWapper queryRowDataWapper;
    private final DataCheckWapper dataCheckWapper;

    public IncrementDataCheckThread(@NonNull DataCheckParam checkParam, @NonNull FeignClientService feignClient) {
        this.topic = checkParam.getTopic();
        this.tableName = topic.getTableName();
        this.partitions = checkParam.getPartitions();
        this.path = checkParam.getPath();
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

        // 元数据校验
        if (!checkTableMetadata()) {
            return;
        }
        // 初次校验
        firstCheckCompare();
        // 解析初次校验结果
        List<String> diffIdList = parseDiffResult();
        // 根据初次校验结果进行二次校验
        secondaryCheckCompare(diffIdList);
        // 校验结果 校验修复报告
        checkResult();
    }

    /**
     * 初次校验
     */
    private void firstCheckCompare() {
        // 初始化桶列表
        initFirstCheckBucketList();
        compareCommonMerkleTree();
    }

    /**
     * 二次校验
     *
     * @param diffIdList 初次校验差异ID列表
     */
    private void secondaryCheckCompare(List<String> diffIdList) {
        if (CollectionUtils.isEmpty(diffIdList)) {
            return;
        }
        // 清理当前线程捏校验缓存信息
        lastDataClean();
        // 初始化桶列表
        initSecondaryCheckBucketList(diffIdList);
        // 进行二次校验
        compareCommonMerkleTree();
    }

    /**
     * 初始化桶列表
     */
    private void initFirstCheckBucketList() {
        // 获取当前任务对应kafka分区号
        // 初始化源端桶列列表数据
        initFirstCheckBucketList(Endpoint.SOURCE, sourceBucketList);
        // 初始化宿端桶列列表数据
        initFirstCheckBucketList(Endpoint.SINK, sinkBucketList);
        // 对齐源端宿端桶列表
        alignAllBuckets();
        // 排序
        sortBuckets(sourceBucketList);
        sortBuckets(sinkBucketList);
    }

    private void initSecondaryCheckBucketList(List<String> diffIdList) {
        SourceDataLog dataLog = new SourceDataLog().setTableName(tableName)
                .setCompositePrimaryValues(diffIdList);
        buildBucket(Endpoint.SOURCE, dataLog);
        buildBucket(Endpoint.SINK, dataLog);
        // 对齐源端宿端桶列表
        alignAllBuckets();
        // 排序
        sortBuckets(sourceBucketList);
        sortBuckets(sinkBucketList);
    }

    private void compareCommonMerkleTree() {
        //不进行默克尔树校验算法场景
        final int sourceBucketCount = sourceBucketList.size();
        final int sinkBucketCount = sinkBucketList.size();
        if (checkNotMerkleCompare(sourceBucketCount, sinkBucketCount)) {
            // 不满足默克尔树约束条件下 比较  sourceSize等于0，即都是空桶
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
        }

        // 构造默克尔树 约束: bucketList 不能为空，且size>=2
        MerkleTree sourceTree = new MerkleTree(sourceBucketList);
        MerkleTree sinkTree = new MerkleTree(sinkBucketList);

        //递归比较两颗默克尔树，并将差异记录返回。
        compareMerkleTree(sourceTree, sinkTree);
    }

    private void lastDataClean() {
        sourceBucketList.clear();
        sinkBucketList.clear();
        difference.getOnlyOnRight().clear();
        difference.getOnlyOnLeft().clear();
        difference.getDiffering().clear();
    }


    /**
     * 根据桶编号对最终桶列表进行排序
     *
     * @param bucketList 桶列表
     */
    private void sortBuckets(@NonNull List<Bucket> bucketList) {
        bucketList.sort(Comparator.comparingInt(Bucket::getNumber));
    }

    private List<String> parseDiffResult() {
        List<String> diffKeyList = new ArrayList<>();
        diffKeyList.addAll(difference.getDiffering().keySet());
        diffKeyList.addAll(difference.getOnlyOnRight().keySet());
        diffKeyList.addAll(difference.getOnlyOnLeft().keySet());
        return diffKeyList;
    }

    /**
     * 增量校验前置条件，当前表结构一致，若表结构不一致则直接退出。不进行数据校验
     *
     * @return 返回元数据校验结果
     */
    private boolean checkTableMetadata() {
        TableMetadataHash sourceTableHash = queryTableMetadataHash(Endpoint.SOURCE, tableName);
        TableMetadataHash sinkTableHash = queryTableMetadataHash(Endpoint.SINK, tableName);
        return Objects.equals(sourceTableHash, sinkTableHash);
    }

    private TableMetadataHash queryTableMetadataHash(Endpoint endpoint, String tableName) {
        Result<TableMetadataHash> result = feignClient.getClient(endpoint).queryTableMetadataHash(tableName);
        if (result.isSuccess()) {
            return result.getData();
        } else {
            throw new DispatchClientException(endpoint, "query table metadata hash " + tableName +
                    " error, " + result.getMessage());
        }
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
        return sourceBucketCount < THRESHOLD_MIN_BUCKET_SIZE || sinkBucketCount < THRESHOLD_MIN_BUCKET_SIZE;
    }

    /**
     * 比较两颗默克尔树，并将差异记录返回。
     *
     * @param sourceTree 源端默克尔树
     * @param sinkTree   宿端默克尔树
     */
    private void compareMerkleTree(@NonNull MerkleTree sourceTree, @NonNull MerkleTree sinkTree) {
        // 默克尔树比较
        if (sourceTree.getDepth() != sinkTree.getDepth()) {
            throw new MerkleTreeDepthException(String.format("source & sink data have large different, Please synchronize data again! " +
                            "merkel tree depth different,source depth=[%d],sink depth=[%d]",
                    sourceTree.getDepth(), sinkTree.getDepth()));
        }

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
     * @param bucketList 桶列表
     */
    private void initFirstCheckBucketList(Endpoint endpoint, List<Bucket> bucketList) {

        // 使用feignclient 拉取kafka数据
        List<RowDataHash> dataList = getTopicPartitionsData(endpoint);
        buildBucket(dataList, endpoint, bucketList);
    }

    private void buildBucket(List<RowDataHash> dataList, Endpoint endpoint, List<Bucket> bucketList) {
        if (CollectionUtils.isEmpty(dataList)) {
            return;
        }
        Map<Integer, Bucket> bucketMap = new HashMap<>();
        BuilderBucketHandler bucketBuilder = new BuilderBucketHandler(bucketCapacity);

        // 拉取的数据进行构建桶列表
        bucketBuilder.builder(dataList, dataList.size(), bucketMap);
        // 统计桶列表信息
        bucketNumberStatisticsIncrement(endpoint, bucketMap.keySet());
        bucketList.addAll(bucketMap.values());
    }

    private void buildBucket(Endpoint endpoint, SourceDataLog dataLog) {
        final List<RowDataHash> dataList = getSecondaryCheckRowData(endpoint, dataLog);
        buildBucket(dataList, endpoint, sourceBucketList);
    }

    /**
     * 对各端点构建的桶编号进行统计。统计结果汇总到{@code bucketNumberDiffMap}中。
     * <p>
     * 默克尔比较算法，需要确保双方桶编号的一致。
     * <p>
     * 如果一方的桶编号存在缺失，即{@code Pair<S,T>}中，S或T的值为-1，则需要生成相应编号的空桶。
     *
     * @param endpoint        端点
     * @param bucketNumberSet 桶编号
     */
    private void bucketNumberStatisticsIncrement(@NonNull Endpoint endpoint, @NonNull Set<Integer> bucketNumberSet) {
        bucketNumberSet.forEach(bucketNumber -> {
            if (!bucketNumberDiffMap.containsKey(bucketNumber)) {
                if (Objects.equals(endpoint, Endpoint.SOURCE)) {
                    bucketNumberDiffMap.put(bucketNumber, Pair.of(bucketNumber, -1));
                } else {
                    bucketNumberDiffMap.put(bucketNumber, Pair.of(-1, bucketNumber));
                }
            } else {
                Pair<Integer, Integer> pair = bucketNumberDiffMap.get(bucketNumber);
                if (Objects.equals(endpoint, Endpoint.SOURCE)) {
                    bucketNumberDiffMap.put(bucketNumber, Pair.of(bucketNumber, pair));
                } else {
                    bucketNumberDiffMap.put(bucketNumber, Pair.of(pair, bucketNumber));
                }
            }
        });
    }

    /**
     * 拉取指定端点{@code endpoint}的表{@code tableName}的 kafka分区{@code partitions}数据
     *
     * @param endpoint 端点类型
     * @return 指定表 kafka分区数据
     */
    private List<RowDataHash> getTopicPartitionsData(Endpoint endpoint) {
        return queryRowDataWapper.queryIncrementRowData(endpoint, tableName);
    }

    private List<RowDataHash> getSecondaryCheckRowData(Endpoint endpoint, SourceDataLog dataLog) {
        return queryRowDataWapper.queryRowData(endpoint, dataLog);
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
        Map<String, Pair<RowDataHash, RowDataHash>> differing = new HashMap<>();
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
     * 若当前遍历的节点{@link Node}签名相同则终止当前遍历分支。
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

    private void checkResult() {
        CheckDiffResult result = AbstractCheckDiffResultBuilder.builder(feignClient)
                .table(tableName)
                .topic(topic.getTopicName())
                .partitions(partitions)
                .keyUpdateSet(difference.getDiffering().keySet())
                .keyInsertSet(difference.getOnlyOnRight().keySet())
                .keyDeleteSet(difference.getOnlyOnLeft().keySet())
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
