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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.common.constant.Constants;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * DataCheckKafkaConsumer
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/14
 * @since ：11
 */
@Slf4j
public class DataCheckKafkaConsumer {
    private KafkaProperties properties;
    private FeignClientService feignClient;

    /**
     * DataCheckKafkaConsumer constructor method
     *
     * @param properties  KafkaProperties
     * @param feignClient FeignClientService
     */
    public DataCheckKafkaConsumer(KafkaProperties properties, FeignClientService feignClient) {
        this.properties = properties;
        this.feignClient = feignClient;
    }

    /**
     * Query the Kafka partition data corresponding to the specified table
     *
     * @param endpoint   endpoint {@value Endpoint#API_DESCRIPTION}
     * @param tableName  table Name
     * @param partitions Kafka partitions
     * @return kafka partitions data
     */
    public List<RowDataHash> queryRowData(Endpoint endpoint, String tableName, int partitions) {
        List<RowDataHash> data = Collections.synchronizedList(new ArrayList<>());
        Topic topic = feignClient.queryTopicInfo(endpoint, tableName);

        KafkaConsumer<String, String> kafkaConsumer = buildKafkaConsumer();
        kafkaConsumer.assign(List.of(new TopicPartition(topic.getTopicName(), partitions)));

        consumerTopicRecords(data, kafkaConsumer);
        if (CollectionUtils.isEmpty(data)) {
            ThreadUtil.sleep(1000);
            consumerTopicRecords(data, kafkaConsumer);
        }
        log.debug("consumer kafka topic=[{}] partitions=[{}] dataList=[{}]", topic.toString(), partitions, data.size());
        return data;
    }

    private void consumerTopicRecords(List<RowDataHash> data, KafkaConsumer<String, String> kafkaConsumer) {
        List<RowDataHash> result = getTopicRecords(kafkaConsumer);
        while (result.size() > 0) {
            data.addAll(result);
            result = getTopicRecords(kafkaConsumer);
        }
    }

    private List<RowDataHash> getTopicRecords(KafkaConsumer<String, String> kafkaConsumer) {
        List<RowDataHash> dataList = new ArrayList<>();
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(200));
        consumerRecords.forEach(record -> {
            dataList.add(JSON.parseObject(record.value(), RowDataHash.class));
        });
        return dataList;
    }

    private KafkaConsumer<String, String> buildKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            String.join(Constants.DELIMITER, properties.getBootstrapServers()));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getConsumer().getGroupId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getConsumer().getAutoOffsetReset());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
