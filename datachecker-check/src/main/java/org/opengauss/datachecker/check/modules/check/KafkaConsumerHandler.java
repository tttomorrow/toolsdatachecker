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

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.util.ThreadUtil;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * KafkaConsumerHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/8/31
 * @since ：11
 */
@Slf4j
public class KafkaConsumerHandler {
    private static final int RETRY_FETCH_RECORD_INTERVAL = 1000;
    private static final int KAFKA_CONSUMER_POLL_DURATION = 20;

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final int retryFetchRecordTimes;

    /**
     * Constructor
     *
     * @param consumer   consumer
     * @param retryTimes retryTimes
     */
    public KafkaConsumerHandler(KafkaConsumer<String, String> consumer, int retryTimes) {
        kafkaConsumer = consumer;
        retryFetchRecordTimes = retryTimes;
    }

    /**
     * Query the Kafka partition data corresponding to the specified table
     *
     * @param topic      Kafka topic
     * @param partitions Kafka partitions
     * @return kafka partitions data
     */
    public List<RowDataHash> queryCheckRowData(Topic topic, int partitions) {
        return queryRowData(topic, partitions, false);
    }

    /**
     * Query the Kafka partition data corresponding to the specified table
     *
     * @param topic                     Kafka topic
     * @param partitions                Kafka partitions
     * @param shouldChangeConsumerGroup if true change consumer Group random
     * @return kafka partitions data
     */
    public List<RowDataHash> queryRowData(Topic topic, int partitions, boolean shouldChangeConsumerGroup) {
        List<RowDataHash> data = Collections.synchronizedList(new LinkedList<>());
        final TopicPartition topicPartition = new TopicPartition(topic.getTopicName(), partitions);
        kafkaConsumer.assign(List.of(topicPartition));
        if (shouldChangeConsumerGroup) {
            resetOffsetToBeginning(kafkaConsumer, topicPartition);
        }
        consumerTopicRecords(data, kafkaConsumer);
        AtomicInteger retryTimes = new AtomicInteger(0);
        final String groupId = kafkaConsumer.groupMetadata().groupId();
        while (CollectionUtils.isEmpty(data) && retryTimes.incrementAndGet() <= retryFetchRecordTimes) {
            ThreadUtil.sleep(RETRY_FETCH_RECORD_INTERVAL);
            resetOffsetToBeginning(kafkaConsumer, topicPartition);
            log.debug("consumer group={} topic=[{}] partitions=[{}] empty ,retryTimes=[{}]", groupId,
                topic.getTopicName(), partitions, retryTimes.get());
            consumerTopicRecords(data, kafkaConsumer);
        }
        log.debug("consumer group={} topic=[{}] partitions=[{}] dataList=[{}]", groupId, topic.getTopicName(),
            partitions, data.size());
        return data;
    }

    private void resetOffsetToBeginning(KafkaConsumer<String, String> consumer, TopicPartition topicPartition) {
        Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
        consumer.seekToBeginning(List.of(topicPartition));
        long position = consumer.position(topicPartition);
        offset.put(topicPartition, new OffsetAndMetadata(position));
        consumer.commitSync(offset);
    }

    private void consumerTopicRecords(List<RowDataHash> data, KafkaConsumer<String, String> kafkaConsumer) {
        List<RowDataHash> result = getTopicRecords(kafkaConsumer);
        while (CollectionUtils.isNotEmpty(result)) {
            data.addAll(result);
            result = getTopicRecords(kafkaConsumer);
        }
    }

    private List<RowDataHash> getTopicRecords(KafkaConsumer<String, String> kafkaConsumer) {
        List<RowDataHash> dataList = new ArrayList<>();
        ConsumerRecords<String, String> consumerRecords =
            kafkaConsumer.poll(Duration.ofMillis(KAFKA_CONSUMER_POLL_DURATION));
        consumerRecords.forEach(record -> {
            dataList.add(JSON.parseObject(record.value(), RowDataHash.class));
        });
        return dataList;
    }
}
