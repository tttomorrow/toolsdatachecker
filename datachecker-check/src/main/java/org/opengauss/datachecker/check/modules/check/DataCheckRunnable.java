/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.check.modules.check;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.check.cache.TableStatusRegister;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.modules.bucket.Bucket;
import org.opengauss.datachecker.check.modules.bucket.BuilderBucketHandler;
import org.opengauss.datachecker.check.modules.merkle.MerkleTree;
import org.opengauss.datachecker.check.modules.merkle.MerkleTree.Node;
import org.opengauss.datachecker.check.service.StatisticalService;
import org.opengauss.datachecker.common.constant.Constants;
import org.opengauss.datachecker.common.entry.check.DataCheckParam;
import org.opengauss.datachecker.common.entry.check.DifferencePair;
import org.opengauss.datachecker.common.entry.check.Pair;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.exception.LargeDataDiffException;
import org.opengauss.datachecker.common.exception.MerkleTreeDepthException;
import org.springframework.lang.NonNull;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DataCheckRunnable
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Slf4j
public class DataCheckRunnable implements Runnable {
    private static final int THRESHOLD_MIN_BUCKET_SIZE = 2;
    private static final String THREAD_NAME_PRIFEX = "DATA_CHECK_";

    private final LocalDateTime start;
    private final List<Bucket> sourceBucketList = Collections.synchronizedList(new ArrayList<>());
    private final List<Bucket> sinkBucketList = Collections.synchronizedList(new ArrayList<>());
    private final DifferencePair<Map<String, RowDataHash>, Map<String, RowDataHash>, Map<String, Pair<Node, Node>>>
            difference = DifferencePair.of(new HashMap<>(), new HashMap<>(), new HashMap<>());
    private final Map<Integer, Pair<Integer, Integer>> bucketNumberDiffMap = new HashMap<>();
    private final FeignClientService feignClient;
    private final StatisticalService statisticalService;
    private final TableStatusRegister tableStatusRegister;
    private final DataCheckParam checkParam;
    private final KafkaConsumerHandler kafkaConsumerHandler;

    private String sinkSchema;
    private Topic topic;
    private String tableName;
    private int partitions;
    private int rowCount;
    private int errorRate;
    private int bucketCapacity;
    private String path;

    /**
     * DataCheckRunnable
     *
     * @param checkParam checkParam
     * @param support    support
     */
    public DataCheckRunnable(@NonNull DataCheckParam checkParam, @NonNull DataCheckRunnableSupport support) {
        this.checkParam = checkParam;
        start = LocalDateTime.now();
        feignClient = support.getFeignClientService();
        statisticalService = support.getStatisticalService();
        tableStatusRegister = support.getTableStatusRegister();
        kafkaConsumerHandler = buildKafkaHandler(support);
    }

    private KafkaConsumerHandler buildKafkaHandler(DataCheckRunnableSupport support) {
        KafkaConsumerService kafkaConsumerService = support.getKafkaConsumerService();
        return new KafkaConsumerHandler(kafkaConsumerService.buildKafkaConsumer(false),
                kafkaConsumerService.getRetryFetchRecordTimes());
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
        try {
            paramInit();
            checkTableData();
        } catch (CheckingException ex) {
            log.error("happen before some error,", ex);
        } finally {
            checkResult();
            statisticalService.statistics(getStatisticsName(tableName, partitions), start);
            cleanCheckThreadEnvironment();
            refreshCheckStatus();
        }
    }

    private void checkTableData() {
        // Initialize bucket list
        initBucketList();
        // No Merkel tree verification algorithm scenario
        if (!shouldCheckMerkleTree(sourceBucketList.size(), sinkBucketList.size())) {
            compareNoMerkleTree(sourceBucketList.size(), sinkBucketList.size());
        } else {
            // Construct Merkel tree constraint: bucketList cannot be empty, and size > =2
            MerkleTree sourceTree = new MerkleTree(sourceBucketList);
            MerkleTree sinkTree = new MerkleTree(sinkBucketList);

            // Merkel tree comparison
            if (sourceTree.getDepth() != sinkTree.getDepth()) {
                refreshCheckStatus();
                throw new MerkleTreeDepthException(String.format(Locale.ROOT,
                        "source & sink data have large different, Please synchronize data again! "
                                + "merkel tree depth different,source depth=[%d],sink depth=[%d]", sourceTree.getDepth(),
                        sinkTree.getDepth()));
            }
            // Recursively compare two Merkel trees and return the difference record.
            compareMerkleTree(sourceTree, sinkTree);
        }
    }

    private void paramInit() {
        sinkSchema = feignClient.getDatabaseSchema(Endpoint.SINK);
        topic = checkParam.getTopic();
        tableName = topic.getTableName();
        partitions = checkParam.getPartitions();
        rowCount = 0;
        errorRate = checkParam.getErrorRate();
        path = checkParam.getPath();
        bucketCapacity = checkParam.getBucketCapacity();
        resetThreadName(tableName, partitions);
    }

    private void refreshCheckStatus() {
        tableStatusRegister.update(tableName, partitions, TableStatusRegister.TASK_STATUS_CHECK_VALUE);
    }

    private static String getStatisticsName(String tableName, int partitions) {
        return tableName.concat("_").concat(String.valueOf(partitions));
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
     * Initialize bucket list
     */
    private void initBucketList() {
        // Get the Kafka partition number corresponding to the current task
        // Initialize source bucket column list data
        initBucketList(Endpoint.SOURCE, partitions, sourceBucketList);
        // Initialize destination bucket column list data
        initBucketList(Endpoint.SINK, partitions, sinkBucketList);
        // Align the source and destination bucket list
        alignAllBuckets();
        sortBuckets(sourceBucketList);
        sortBuckets(sinkBucketList);
        log.info("Initialize the verification data and the bucket construction is currently completed of table [{}-{}]",
                tableName, partitions);
    }

    /**
     * Sort the final bucket list by bucket number
     *
     * @param bucketList bucketList
     */
    private void sortBuckets(@NonNull List<Bucket> bucketList) {
        bucketList.sort(Comparator.comparingInt(Bucket::getNumber));
    }

    /**
     * <pre>
     * Align the bucket list data according to the statistical results of source
     * and destination bucket difference information {@code bucketNumberDiffMap}.
     * </pre>
     */
    private void alignAllBuckets() {
        new DataCheckWapper().alignAllBuckets(bucketNumberDiffMap, sourceBucketList, sinkBucketList);
    }

    /**
     * Pull the Kafka partition {@code partitions} data
     * of the specified table {@code tableName} of the specified endpoint {@code endpoint} service.
     * <p>
     * And assemble Kafka data into the specified bucket list {@code bucketList}
     *
     * @param endpoint   Endpoint Type
     * @param partitions kafka partitions
     * @param bucketList Bucket list
     */
    private void initBucketList(Endpoint endpoint, int partitions, List<Bucket> bucketList) {
        Map<Integer, Bucket> bucketMap = new ConcurrentHashMap<>(Constants.InitialCapacity.EMPTY);
        // Use feign client to pull Kafka data
        List<RowDataHash> dataList = getTopicPartitionsData(endpoint, partitions);
        rowCount = rowCount + dataList.size();
        if (CollectionUtils.isEmpty(dataList)) {
            return;
        }
        log.info("Initialize the verification thread data, and pull the total number of [{}-{}-{}] data records to {}",
                endpoint.getDescription(), tableName, partitions, dataList.size());
        BuilderBucketHandler bucketBuilder = new BuilderBucketHandler(bucketCapacity);

        // Use the pulled data to build the bucket list
        bucketBuilder.builder(dataList, dataList.size(), bucketMap);
        // Statistics bucket list information
        bucketNoStatistics(endpoint, bucketMap.keySet());
        bucketList.addAll(bucketMap.values());
    }

    /**
     * Compare the two Merkel trees and return the difference record.
     *
     * @param sourceTree Source Merkel tree
     * @param sinkTree   Sink Merkel tree
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
        log.info("Complete the data verification of table [{}-{}]", tableName, partitions);
    }

    /**
     * Compare the difference data recorded inside the two barrels
     * <p>
     *
     * @param sourceBucket Source barrel
     * @param sinkBucket   Sink barrel
     * @return Difference record
     */
    private DifferencePair<Map, Map, Map> compareBucket(Bucket sourceBucket, Bucket sinkBucket) {
        Map<String, RowDataHash> sourceMap = sourceBucket.getBucket();
        Map<String, RowDataHash> sinkMap = sinkBucket.getBucket();
        MapDifference<String, RowDataHash> bucketDifference = Maps.difference(sourceMap, sinkMap);
        Map<String, RowDataHash> entriesOnlyOnLeft = bucketDifference.entriesOnlyOnLeft();
        Map<String, RowDataHash> entriesOnlyOnRight = bucketDifference.entriesOnlyOnRight();
        Map<String, MapDifference.ValueDifference<RowDataHash>> entriesDiffering = bucketDifference.entriesDiffering();
        Map<String, Pair<RowDataHash, RowDataHash>> differing = new HashMap<>(Constants.InitialCapacity.EMPTY);
        entriesDiffering.forEach((key, diff) -> {
            differing.put(key, Pair.of(diff.leftValue(), diff.rightValue()));
        });
        return DifferencePair.of(entriesOnlyOnLeft, entriesOnlyOnRight, differing);
    }

    /**
     * <pre>
     * Recursively compare two Merkel tree nodes and record the difference nodes.
     * The recursive preorder traversal method is adopted to traverse and compare the Merkel tree,
     * so as to find the difference node.
     * If the current traversal node {@link org.opengauss.datachecker.check.modules.merkle.MerkleTree.Node}
     * has the same signature, the current traversal branch will be terminated.
     * </pre>
     *
     * @param source       Source Merkel tree node
     * @param sink         Sink Merkel tree node
     * @param diffNodeList Difference node record
     */
    private void compareMerkleTree(Node source, Node sink, List<Pair<Node, Node>> diffNodeList) {
        // If the nodes are the same, exit
        if (Objects.isNull(source) || Objects.isNull(sink)) {
            return;
        }
        if (Arrays.equals(source.getSignature(), sink.getSignature())) {
            return;
        }
        // If the nodes are different, continue to compare the lower level nodes.
        // If the current difference node is a leaf node, record the difference node and exit
        if (source.getType() == MerkleTree.LEAF_SIG_TYPE) {
            diffNodeList.add(Pair.of(source, sink));
            return;
        }
        compareMerkleTree(source.getLeft(), sink.getLeft(), diffNodeList);
        compareMerkleTree(source.getRight(), sink.getRight(), diffNodeList);
    }

    /**
     * <pre>
     * Count the bucket numbers built at each endpoint.
     * The statistical results are summarized in {@code bucketNumberDiffMap}.
     * Merkel 's comparison algorithm needs to ensure that the bucket numbers of both sides are consistent.
     * If the bucket number of one party is missing, that is, in {@code Pair<s, t >}, the value of S or T is -1,
     * you need to generate an empty bucket with the corresponding number.
     * </pre>
     *
     * @param endpoint    end point
     * @param bucketNoSet bucket numbers
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
     * Pull the Kafka partition {@code partitions} data
     * of the table {@code tableName} of the specified endpoint {@code endpoint}
     *
     * @param endpoint   endpoint
     * @param partitions kafka partitions
     * @return Specify table Kafka partition data
     */
    private List<RowDataHash> getTopicPartitionsData(Endpoint endpoint, int partitions) {
        Topic endpointTopic = feignClient.queryTopicInfo(endpoint, tableName);
        return kafkaConsumerHandler.queryCheckRowData(endpointTopic, partitions);
    }

    private boolean shouldCheckMerkleTree(int sourceBucketCount, int sinkBucketCount) {
        return sourceBucketCount >= THRESHOLD_MIN_BUCKET_SIZE && sinkBucketCount >= THRESHOLD_MIN_BUCKET_SIZE;
    }

    /**
     * Comparison under Merkel tree constraints
     *
     * @param sourceBucketCount source bucket count
     * @param sinkBucketCount   sink bucket count
     * @return Whether it meets the Merkel verification scenario
     */
    private void compareNoMerkleTree(int sourceBucketCount, int sinkBucketCount) {
        // Comparison without Merkel tree constraint
        if (sourceBucketCount == sinkBucketCount) {
            // sourceSize == 0, that is, all buckets are empty
            if (sourceBucketCount == 0) {
                // Table is empty, verification succeeded!
                log.info("table[{}-{}] is an empty table,this check successful!", tableName, partitions);
            } else {
                // sourceSize is less than thresholdMinBucketSize, that is, there is only one bucket. Compare
                DifferencePair<Map, Map, Map> subDifference =
                        compareBucket(sourceBucketList.get(0), sinkBucketList.get(0));
                difference.getDiffering().putAll(subDifference.getDiffering());
                difference.getOnlyOnLeft().putAll(subDifference.getOnlyOnLeft());
                difference.getOnlyOnRight().putAll(subDifference.getOnlyOnRight());
            }
            refreshCheckStatus();
        } else {
            refreshCheckStatus();
            throw new LargeDataDiffException(String.format(
                    "table[%s] source & sink data have large different," + "source-bucket-count=[%s] sink-bucket-count=[%s]"
                            + " Please synchronize data again! ", tableName, sourceBucketCount, sinkBucketCount));
        }
    }

    private void checkResult() {
        CheckDiffResult result =
                AbstractCheckDiffResultBuilder.builder(feignClient).table(tableName).topic(topic.getTopicName())
                        .schema(sinkSchema).partitions(partitions).isTableStructureEquals(true)
                        .isExistTableMiss(false, null)
                        .rowCount(rowCount).errorRate(20)
                        .keyUpdateSet(difference.getDiffering().keySet())
                        .keyInsertSet(difference.getOnlyOnLeft().keySet())
                        .keyDeleteSet(difference.getOnlyOnRight().keySet()).build();
        ExportCheckResult.export(path, result);
        log.info("Complete the output of data verification results of table [{}-{}]", tableName, partitions);
    }

    private void resetThreadName(String tableName, int partitions) {
        Thread.currentThread().setName(THREAD_NAME_PRIFEX + tableName + "_" + partitions);
    }
}
