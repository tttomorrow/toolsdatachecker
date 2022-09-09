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

package org.opengauss.datachecker.extract.debe;

import com.alibaba.fastjson.JSONException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.opengauss.datachecker.common.entry.check.IncrementCheckConfig;
import org.opengauss.datachecker.common.entry.check.IncrementCheckTopic;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.exception.DebeziumConfigException;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.config.KafkaConsumerConfig;
import org.opengauss.datachecker.extract.kafka.KafkaAdminService;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DataConsolidationServiceImpl
 *
 * @author ：zhangyaozhong
 * @date ：Created in 2022/6/14
 * @since ：11
 */
@Slf4j
@Service
public class DataConsolidationServiceImpl implements DataConsolidationService {
    private static final IncrementCheckConfig INCREMENT_CHECK_CONIFG = new IncrementCheckConfig();

    private final Object lock = new Object();
    private final DebeziumDataHandler debeziumDataHandler = new DebeziumDataHandler();

    private KafkaConsumer<String, String> debeziumTopicOffSetConsumer = null;

    @Autowired
    private KafkaConsumerConfig consumerConfig;
    @Autowired
    private KafkaAdminService kafkaAdminService;
    @Autowired
    private ExtractProperties extractProperties;
    @Autowired
    private MetaDataService metaDataService;

    /**
     * initIncrementConfig
     */
    @PostConstruct
    public void initIncrementConfig() {
        if (extractProperties.isDebeziumEnable()) {
            metaDataService.init();
            INCREMENT_CHECK_CONIFG.setDebeziumTopic(extractProperties.getDebeziumTopic())
                                  .setDebeziumTables(extractProperties.getDebeziumTables())
                                  .setPartitions(extractProperties.getDebeziumTopicPartitions())
                                  .setGroupId(extractProperties.getDebeziumGroupId());
            getDebeziumTopicRecordOffSet();
        }
    }

    /**
     * Get the topic records of debezium, and analyze and merge the topic records
     *
     * @param topicName topic name
     * @return topic records
     */
    @Override
    public List<SourceDataLog> getDebeziumTopicRecords(String topicName) {
        checkIncrementCheckEnvironment();
        IncrementCheckTopic topic = getDebeziumTopic();
        // if test service reset the group id is  IdGenerator.nextId36()
        topic.setTopic(topicName);
        KafkaConsumer<String, String> kafkaConsumer = consumerConfig.getDebeziumConsumer(topic.getGroupId());
        kafkaConsumer.subscribe(List.of(topicName));
        log.info("kafka debezium topic consumer topic=[{}]", topicName);
        // Consume a partition data of a topic
        List<SourceDataLog> dataList = new ArrayList<>();
        consumerAllRecords(kafkaConsumer, dataList);
        log.info("kafka consumer topic=[{}] dataList=[{}]", topicName, dataList.size());
        return dataList;
    }

    private void consumerAllRecords(KafkaConsumer<String, String> kafkaConsumer, List<SourceDataLog> dataList) {
        log.debug("kafka Consumer poll");
        DebeziumDataLogs debeziumDataLogs = new DebeziumDataLogs();
        int consumerRecords = getConsumerRecords(kafkaConsumer, debeziumDataLogs);
        while (consumerRecords > 0) {
            consumerRecords = getConsumerRecords(kafkaConsumer, debeziumDataLogs);
        }
        dataList.addAll(debeziumDataLogs.values());
        log.debug("Consumer data debezium data handler");
    }

    /**
     * Consume and process Kafka consumer client data
     *
     * @param kafkaConsumer    consumer
     * @param debeziumDataLogs Processing results
     * @return Number of consumer records
     */
    private int getConsumerRecords(KafkaConsumer<String, String> kafkaConsumer, DebeziumDataLogs debeziumDataLogs) {
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(200));
        consumerRecords.forEach(record -> {
            try {
                debeziumDataHandler.handler(record.value(), debeziumDataLogs);
            } catch (DebeziumConfigException | JSONException ex) {
                // Abnormal message structure, ignoring the current message
                log.error("Abnormal message structure, ignoring the current message,{},{}", record.value(),
                    ex.getMessage());
            }
        });
        return consumerRecords.count();
    }

    /**
     * Get the message record information of the topic corresponding to the debrizum listening table
     *
     * @return Return message consumption record
     */
    @Override
    public IncrementCheckTopic getDebeziumTopicRecordOffSet() {
        checkIncrementCheckEnvironment();
        IncrementCheckTopic topic = getDebeziumTopic();
        final TopicPartition topicPartition = new TopicPartition(topic.getTopic(), 0);
        List<TopicPartition> partitionList = List.of(topicPartition);
        debeziumTopicOffSetConsumer = getDebeziumTopicOffSetConsumer();

        // View topic current message consumption starting position
        debeziumTopicOffSetConsumer.seekToBeginning(partitionList);
        topic.setBegin(debeziumTopicOffSetConsumer.position(topicPartition));

        // View topic current message consumption deadline
        debeziumTopicOffSetConsumer.seekToEnd(partitionList);
        topic.setEnd(debeziumTopicOffSetConsumer.position(topicPartition));
        return topic;
    }

    private KafkaConsumer<String, String> getDebeziumTopicOffSetConsumer() {
        if (Objects.nonNull(debeziumTopicOffSetConsumer)) {
            return debeziumTopicOffSetConsumer;
        } else {
            synchronized (lock) {
                if (Objects.isNull(debeziumTopicOffSetConsumer)) {
                    IncrementCheckTopic topic = getDebeziumTopic();
                    final TopicPartition topicPartition = new TopicPartition(topic.getTopic(), 0);
                    debeziumTopicOffSetConsumer = consumerConfig.getDebeziumConsumer(topic.getGroupId());
                    List<TopicPartition> partitionList = List.of(topicPartition);
                    // Set consumption mode as partition
                    debeziumTopicOffSetConsumer.assign(partitionList);
                }
            }
        }
        return debeziumTopicOffSetConsumer;
    }

    /**
     * Get the debezium listening table and record the offset information of the message corresponding to the topic
     *
     * @return offset
     */
    @Override
    public long getDebeziumTopicRecordEndOffSet() {
        final TopicPartition topicPartition = new TopicPartition(INCREMENT_CHECK_CONIFG.getDebeziumTopic(), 0);
        // View topic current message consumption deadline
        return debeziumTopicOffSetConsumer.position(topicPartition);
    }

    private IncrementCheckTopic getDebeziumTopic() {
        return new IncrementCheckTopic().setTopic(INCREMENT_CHECK_CONIFG.getDebeziumTopic())
                                        .setGroupId(INCREMENT_CHECK_CONIFG.getGroupId())
                                        .setPartitions(INCREMENT_CHECK_CONIFG.getPartitions());
    }

    @Override
    public boolean isSourceEndpoint() {
        return Objects.equals(Endpoint.SOURCE, extractProperties.getEndpoint());
    }

    /**
     * Is the current service a source service
     */
    private void checkSourceEndpoint() {
        Assert.isTrue(isSourceEndpoint(), "The current service is not a source-endpoint-service");
    }

    /**
     * Check the configuration of the debezium environment for incremental verification
     */
    private void checkIncrementCheckEnvironment() {
        final Set<String> allKeys = metaDataService.queryMetaDataOfSchema().keySet();
        // Debezium environmental inspection
        checkDebeziumEnvironment(INCREMENT_CHECK_CONIFG.getDebeziumTopic(), INCREMENT_CHECK_CONIFG.getDebeziumTables(),
            allKeys);
    }

    /**
     * Check and configure the incremental verification debezium environment configuration
     *
     * @param config configuration information
     */
    @Override
    public void configIncrementCheckEnvironment(@NotNull IncrementCheckConfig config) {
        final Set<String> allKeys = MetaDataCache.getAllKeys();
        checkDebeziumEnvironment(config.getDebeziumTopic(), config.getDebeziumTables(), allKeys);
        INCREMENT_CHECK_CONIFG.setDebeziumTables(config.getDebeziumTables()).setDebeziumTopic(config.getDebeziumTopic())
                              .setGroupId(config.getGroupId()).setPartitions(config.getPartitions());
    }

    /**
     * debezium Environment check<p>
     * Check whether the debezium configuration topic exists<p>
     * Check whether the debezium configuration tables exist<p>
     *
     * @param debeziumTopic  Topic to be checked
     * @param debeziumTables Debezium configuration table list
     * @param allTableSet    Source end table set
     */
    private void checkDebeziumEnvironment(
        @NotEmpty(message = "Debezium configuration topic cannot be empty") String debeziumTopic,
        @NotEmpty(message = "Debezium configuration tables cannot be empty") List<String> debeziumTables,
        @NotEmpty(message = "Source side table metadata cache exception") Set<String> allTableSet) {
        checkSourceEndpoint();
        if (!kafkaAdminService.isTopicExists(debeziumTopic)) {
            // The configuration item debezium topic information does not exist
            throw new DebeziumConfigException("The configuration item debezium topic information does not exist");
        }
        final List<String> allTableList = allTableSet.stream().map(String::toUpperCase).collect(Collectors.toList());
        List<String> invalidTables =
            debeziumTables.stream().map(String::toUpperCase).filter(table -> !allTableList.contains(table))
                          .collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(invalidTables)) {
            // The configuration item debezium tables contains non-existent or black and white list tables
            throw new DebeziumConfigException(
                "The configuration item debezium-tables contains non-existent or black-and-white list tables:"
                    + invalidTables.toString());
        }
    }
}
