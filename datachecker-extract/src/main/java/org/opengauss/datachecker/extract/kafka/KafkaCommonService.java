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
import org.opengauss.datachecker.common.util.TopicUtil;
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
                                       .setPartitions(TopicUtil.calcPartitions(divisions));
                    TABLE_TOPIC_CACHE.put(tableName, topic);
                }
            }
        }
        log.debug("kafka topic info : [{}]  ", topic.toString());
        return topic;
    }

    private String createTopicName(String process, String tableName) {
        final Endpoint endpoint = extractProperties.getEndpoint();
        return TopicUtil.buildTopicName(process, endpoint, tableName);
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
}
