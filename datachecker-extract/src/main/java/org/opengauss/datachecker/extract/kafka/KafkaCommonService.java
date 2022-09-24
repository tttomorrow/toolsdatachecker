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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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
     * Rules for generating topic names for full verification
     * process_endpoint_tableName_code
     * The second  %  is the process verification process number
     * The first % is the endpoint {@link Endpoint}
     * table name
     * Last splicing table name upper or lower code
     */
    private static final String TOPIC_TEMPLATE = "%s_%s_%s_%s";
    private static final String UPPER_CODE = "1";
    private static final String LOWER_CODE = "0";

    /**
     * Incremental verification topic prefix
     */
    private static final String INCREMENT_TOPIC_PREFIX = "increment_";
    private static final Object LOCK = new Object();
    private static final Map<String, Topic> TABLE_TOPIC_CACHE = new HashMap<>();

    @Autowired
    private final ExtractProperties extractProperties;

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
                    topic = new Topic().setTableName(tableName).setTopicName(createTopicName(process, tableName))
                                       .setPartitions(calcPartitions(divisions));
                    TABLE_TOPIC_CACHE.put(tableName, topic);
                }
            }
        }
        log.debug("kafka topic info : [{}]  ", topic.toString());
        return topic;
    }

    private String createTopicName(String process, String tableName) {
        final Endpoint endpoint = extractProperties.getEndpoint();
        return String.format(TOPIC_TEMPLATE, process, endpoint.getCode(), tableName, letterCaseEncoding(tableName));
    }

    private String letterCaseEncoding(String tableName) {
        final char[] chars = tableName.toCharArray();
        StringBuilder builder = new StringBuilder();
        for (char aChar : chars) {
            if (aChar >= 'A' && aChar <= 'Z') {
                builder.append(UPPER_CODE);
            } else if (aChar >= 'a' && aChar <= 'z') {
                builder.append(LOWER_CODE);
            }
        }
        final String encoding = builder.toString();
        if (encoding.contains(UPPER_CODE) && encoding.contains(LOWER_CODE)) {
            return encoding;
        }
        return "";
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
    public List<String> getAllTopicName() {
        return TABLE_TOPIC_CACHE.values().stream().map(Topic::getTopicName).collect(Collectors.toList());
    }

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
        Topic topic = new Topic();
        topic.setTableName(tableName).setTopicName(getIncrementTopicName(tableName)).setPartitions(1);
        log.debug("kafka topic info : [{}]  ", topic.toString());
        return topic;
    }

    private String getIncrementTopicName(String tableName) {
        return INCREMENT_TOPIC_PREFIX.concat(Integer.toString(extractProperties.getEndpoint().getCode())).concat("_")
                                     .concat(tableName).toLowerCase();
    }
}
