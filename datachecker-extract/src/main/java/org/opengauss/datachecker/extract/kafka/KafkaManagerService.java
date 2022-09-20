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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.extract.config.KafkaConsumerConfig;
import org.opengauss.datachecker.extract.config.KafkaProducerConfig;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * KafkaManagerService
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/10
 * @since ：11
 */
@Slf4j
@RequiredArgsConstructor
@Service
public class KafkaManagerService {
    private final KafkaAdminService kafkaAdminService;
    private final KafkaCommonService kafkaCommonService;
    private final KafkaConsumerConfig kafkaConsumerConfig;
    private final KafkaProducerConfig kafkaProducerConfig;

    /**
     * Create a topic according to the table name
     *
     * @param process    process
     * @param tableName  tableName
     * @param partitions Total partitions
     * @return Topic name after successful creation
     */
    public String createTopic(String process, String tableName, int partitions) {
        final Topic topicInfo = kafkaCommonService.getTopicInfo(process, tableName, partitions);
        return kafkaAdminService.createTopic(topicInfo.getTopicName(), partitions);
    }

    /**
     * Clear Kafka information
     *
     * @param processNo processNo
     */
    public void cleanKafka(String processNo) {
        kafkaCommonService.cleanTopicMapping();
        log.info("Extract service cleanup Kafka topic mapping information");
        kafkaConsumerConfig.cleanKafkaConsumer();
        log.info("Extract service to clean up Kafka consumer information");
        kafkaProducerConfig.cleanKafkaProducer();
        log.info("Extract service cleanup Kafka producer mapping information");
        List<String> topics = kafkaAdminService.getAllTopic(processNo);
        kafkaAdminService.deleteTopic(topics);
        log.info("Extract service cleanup current process ({}) Kafka topics {}", processNo, topics);
        kafkaAdminService.deleteTopic(topics);
        log.info("Extract service cleanup current process ({}) Kafka topics {}", processNo, topics);
    }

    /**
     * Clear Kafka information
     */
    public void cleanKafka() {
        final List<String> topics = kafkaCommonService.getAllTopicName();
        kafkaConsumerConfig.cleanKafkaConsumer();
        kafkaProducerConfig.cleanKafkaProducer();
        kafkaAdminService.deleteTopic(topics);
        kafkaCommonService.cleanTopicMapping();
    }

    /**
     * Clean up all topics with prefix 前缀TOPIC_EXTRACT_Endpoint_process_ in Kafka
     *
     * @param processNo process
     */
    public void deleteTopic(String processNo) {
        List<String> topics = kafkaAdminService.getAllTopic(processNo);
        kafkaAdminService.deleteTopic(topics);
    }

    /**
     * Clean up all topics
     */
    public void deleteTopic() {
        List<String> topics = kafkaAdminService.getAllTopic();
        kafkaAdminService.deleteTopic(topics);
    }

    /**
     * Query the topic information of the specified table name
     *
     * @param tableName tableName
     * @return topic information
     */
    public Topic getTopic(String tableName) {
        return kafkaCommonService.getTopic(tableName);
    }

    /**
     * Query the topic information of the specified table name
     *
     * @param tableName tableName
     * @return topic information
     */
    public Topic getIncrementTopicInfo(String tableName) {
        return kafkaCommonService.getIncrementTopicInfo(tableName);
    }

    /**
     * Delete the specified topic
     *
     * @param topicName topicName
     */
    public void deleteTopicByName(String topicName) {
        kafkaAdminService.deleteTopic(List.of(topicName));
    }
}
