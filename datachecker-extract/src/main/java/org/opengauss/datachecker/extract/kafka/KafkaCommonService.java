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
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * KafkaCommonService
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/17
 * @since ：11
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaCommonService {
    /**
     * Full calibration extraction topic name template TOPIC_EXTRACT_%s_%s_ <p>
     * The first % is the endpoint {@link Endpoint}
     * The second  %  is the process verification process number
     * Last splicing table name
     */
    private static final String TOPIC_PROCESS_PREFIX = "TOPIC_EXTRACT_%s_%s_";

    /**
     * Full calibration extraction topic name template TOPIC_EXTRACT_%s_ <p>
     * The first % is the endpoint {@link Endpoint}
     * Used to batch query all topic names created by the verification process in Kafka
     */
    private static final String TOPIC_PREFIX_PR = "TOPIC_EXTRACT_%s_";
    private static final String TOPIC_PREFIX = "TOPIC_EXTRACT_";

    /**
     * Incremental verification topic prefix
     */
    private static final String INCREMENT_TOPIC_PREFIX = "TOPIC_EXTRACT_INCREMENT_";
    private static final Object LOCK = new Object();
    private static final Map<String, Topic> TABLE_TOPIC_CACHE = new HashMap<>();
    private static final Map<String, Topic> DEBEZIUM_TOPIC_CACHE = new HashMap<>();

    private final ExtractProperties extractProperties;

    /**
     * Get data verification Kafka topic prefix
     *
     * @param process Verification process No
     * @return topic prefix
     */
    public String getTopicPrefixProcess(String process) {
        return String.format(TOPIC_PROCESS_PREFIX, extractProperties.getEndpoint().getCode(), process);
    }

    /**
     * Get data verification Kafka topic prefix
     *
     * @return Data verification Kafka topic prefix
     */
    public String getTopicPrefixEndpoint() {
        return String.format(TOPIC_PREFIX_PR, extractProperties.getEndpoint().getCode());
    }

    /**
     * Get data verification Kafka topic prefix
     *
     * @return topic prefix
     */
    public String getTopicPrefix() {
        return TOPIC_PREFIX;
    }

    /**
     * Get the corresponding topic according to the table name
     *
     * @param tableName tableName
     * @return topic
     */
    public Topic getTopic(@NonNull String tableName) {
        return TABLE_TOPIC_CACHE.get(tableName);
    }

    /**
     * Obtain the corresponding topic according to relevant information
     *
     * @param process   process
     * @param tableName tableName
     * @param divisions divisions
     * @return topic
     */
    public Topic getTopicInfo(String process, @NonNull String tableName, int divisions) {
        Topic topic = TABLE_TOPIC_CACHE.get(tableName);
        if (Objects.isNull(topic)) {
            synchronized (LOCK) {
                topic = TABLE_TOPIC_CACHE.get(tableName);
                if (Objects.isNull(topic)) {
                    topic = new Topic().setTableName(tableName).setTopicName(
                        getTopicPrefixProcess(process).concat(tableName.toUpperCase(Locale.ROOT)))
                                       .setPartitions(calcPartitions(divisions));
                    TABLE_TOPIC_CACHE.put(tableName, topic);
                }
            }
        }
        log.debug("kafka topic info : [{}]  ", topic.toString());
        return topic;
    }

    /**
     * Calculate the Kafka partition according to the total number of task slices.
     * The total number of Kafka partitions shall not exceed 10
     *
     * @param divisions Number of task slices extracted
     * @return Total number of Kafka partitions
     */
    public int calcPartitions(int divisions) {
        return Math.min(divisions, 10);
    }

    /**
     * Clean up table name and topic information
     */
    public void cleanTopicMapping() {
        TABLE_TOPIC_CACHE.clear();
        log.info("clear table topic cache information");
    }

    /**
     * Get incremental topic information
     *
     * @param tableName tableName
     * @return Topic
     */
    public Topic getIncrementTopicInfo(String tableName) {
        Topic topic = TABLE_TOPIC_CACHE.get(tableName);
        if (Objects.isNull(topic)) {
            synchronized (LOCK) {
                topic = TABLE_TOPIC_CACHE.get(tableName);
                if (Objects.isNull(topic)) {
                    topic = new Topic().setTableName(tableName).setTopicName(getIncrementTopicName(tableName))
                                       .setPartitions(1);
                    TABLE_TOPIC_CACHE.put(tableName, topic);
                }
            }
        }
        log.debug("kafka topic info : [{}]  ", topic.toString());
        return topic;
    }

    private String getIncrementTopicName(String tableName) {
        return INCREMENT_TOPIC_PREFIX.concat(Integer.toString(extractProperties.getEndpoint().getCode())).concat("_")
                                     .concat(tableName.toUpperCase(Locale.ROOT));
    }
}
