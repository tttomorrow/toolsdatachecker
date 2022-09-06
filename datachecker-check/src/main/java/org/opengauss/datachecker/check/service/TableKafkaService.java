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

package org.opengauss.datachecker.check.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.check.modules.check.KafkaConsumerService;
import org.opengauss.datachecker.common.entry.check.TopicRecordInfo;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * TableKafkaService
 *
 * @author ：wangchao
 * @date ：Created in 2022/8/31
 * @since ：11
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TableKafkaService {
    private final KafkaConsumerService kafkaConsumerService;

    /**
     * getTableKafkaConsumerInfo
     *
     * @param topicName      topicName
     * @param partitionTotal partitionTotal
     * @return table kafka info
     */
    public List<TopicRecordInfo> getTableKafkaConsumerInfo(String topicName, int partitionTotal) {
        List<TopicRecordInfo> list = new ArrayList<>();
        Topic topic = new Topic().setTopicName(topicName).setPartitions(partitionTotal);
        IntStream.range(0, topic.getPartitions()).forEach(partitions -> {
            final List<RowDataHash> rowDataHashes = kafkaConsumerService.queryRowData(topic, partitions, true);
            log.info("topic={},partitions={} record-size={}", topic.getTopicName(), partitions, rowDataHashes.size());
            final TopicRecordInfo recordInfo =
                new TopicRecordInfo().setTopic(topic.getTopicName()).setPartitions(partitions)
                                     .setRecordSize(rowDataHashes.size());
            list.add(recordInfo);
        });
        return list;
    }
}
