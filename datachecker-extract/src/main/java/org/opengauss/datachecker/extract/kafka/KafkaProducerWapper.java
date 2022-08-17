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

package org.opengauss.datachecker.extract.kafka;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.extract.config.KafkaProducerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * KafkaProducerWapper
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/14
 * @since ：11
 */
@Slf4j
public class KafkaProducerWapper {
    private static final int FLUSH_KAFKA_PARALLEL_THRESHOLD = 1000;
    private static final int DEFAULT_PARTITION = 0;
    private static final int MIN_PARTITION_NUM = 1;

    private KafkaProducerConfig kafkaProducerConfig;

    /**
     * KafkaProducerWapper build
     *
     * @param config config
     */
    public KafkaProducerWapper(KafkaProducerConfig config) {
        kafkaProducerConfig = config;
    }

    /**
     * Push the data to the topic corresponding to the specified table in batch
     *
     * @param topic          topic
     * @param recordHashList data
     */
    public void syncSend(Topic topic, List<RowDataHash> recordHashList) {
        final int partitions = topic.getPartitions();
        if (partitions <= MIN_PARTITION_NUM) {
            sendRecordToSinglePartitionTopic(recordHashList, topic.getTopicName());
        } else {
            sendMultiPartitionTopic(recordHashList, topic.getTopicName(), partitions);
        }
    }

    private void sendRecordToSinglePartitionTopic(List<RowDataHash> recordHashList, String topicName) {
        final KafkaProducer<String, String> kafkaProducer = kafkaProducerConfig.getKafkaProducer(topicName);
        AtomicInteger cnt = new AtomicInteger(0);
        recordHashList.forEach(record -> {
            record.setPartition(DEFAULT_PARTITION);
            final ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topicName, DEFAULT_PARTITION, record.getPrimaryKey(), JSON.toJSONString(record));
            sendMessage(kafkaProducer, producerRecord, cnt);
        });
        kafkaProducer.flush();
        log.info("send topic={}, record size :{},cnt:{}", topicName, recordHashList.size(), cnt.get());
    }

    private void sendMultiPartitionTopic(List<RowDataHash> recordHashList, String topicName, int partitions) {
        final KafkaProducer<String, String> kafkaProducer = kafkaProducerConfig.getKafkaProducer(topicName);
        AtomicInteger cnt = new AtomicInteger(0);
        List<ProducerRecord> kafkaRecordList = new ArrayList<>();
        recordHashList.forEach(record -> {
            int partition = calcSimplePartition(record.getPrimaryKeyHash(), partitions);
            record.setPartition(partition);
            ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topicName, partition, record.getPrimaryKey(), JSON.toJSONString(record));
            kafkaRecordList.add(producerRecord);
            sendMessage(kafkaProducer, producerRecord, cnt);
        });
        kafkaProducer.flush();
    }

    private int calcSimplePartition(long value, int mod) {
        return (int) Math.abs(value % mod);
    }

    private void sendMessage(KafkaProducer<String, String> kafkaProducer, ProducerRecord<String, String> producerRecord,
        AtomicInteger cnt) {
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("send failed,topic={},key:{} ,partition:{},offset:{}", metadata.topic(), producerRecord.key(),
                    metadata.partition(), metadata.offset(), exception);
            }
        });
        if (cnt.incrementAndGet() % FLUSH_KAFKA_PARALLEL_THRESHOLD == 0) {
            kafkaProducer.flush();
        }
    }
}
