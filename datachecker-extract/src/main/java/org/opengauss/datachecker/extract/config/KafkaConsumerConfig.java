package org.opengauss.datachecker.extract.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opengauss.datachecker.common.entry.check.IncrementCheckTopic;
import org.opengauss.datachecker.extract.constants.ExtConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/17
 * @since ：11
 */
@Slf4j
@Component
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaConsumerConfig {

    private static final Object LOCK = new Object();
    private static final Map<String, KafkaConsumer<String, String>> CONSUMER_MAP = new ConcurrentHashMap<>();

    @Autowired
    private KafkaProperties properties;

    /**
     * Obtaining a specified consumer client based on topic.
     *
     * @param topic      topic name
     * @param partitions total number of partitions
     * @return the topic corresponds to the consumer client.
     */
    public KafkaConsumer<String, String> getKafkaConsumer(String topic, int partitions) {
        String consumerKey = topic + "_" + partitions;
        KafkaConsumer<String, String> consumer = CONSUMER_MAP.get(consumerKey);
        if (Objects.isNull(consumer)) {
            synchronized (LOCK) {
                consumer = CONSUMER_MAP.get(consumerKey);
                if (Objects.isNull(consumer)) {
                    consumer = buildKafkaConsumer();
                    CONSUMER_MAP.put(consumerKey, consumer);
                }
            }
        }
        return consumer;
    }

    public KafkaConsumer<String, String> getDebeziumConsumer(IncrementCheckTopic topic) {
        // configuration information
        Properties props = new Properties();
        // kafka server address
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(ExtConstants.DELIMITER, properties.getBootstrapServers()));
        // consumer group must be specified
        props.put(ConsumerConfig.GROUP_ID_CONFIG, topic.getGroupId());
        // if there are committed offsets in each partition,consumption starts from the submitted offsets.
        // when there is no submitted offset,consumption is started from the beginning
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getConsumer().getAutoOffsetReset());
        // sets the serialization processing class for data keys and values.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // creating a kafka consumer instance
        return new KafkaConsumer<>(props);
    }

    private KafkaConsumer<String, String> buildKafkaConsumer() {
        // configuration information
        Properties props = new Properties();
        // kafka server address
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(ExtConstants.DELIMITER, properties.getBootstrapServers()));
        // consumer group must be specified
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getConsumer().getGroupId());
        // if there are committed offsets in each partition,consumption starts from the submitted offsets.
        // when there is no submitted offset,consumption is started from the beginning
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getConsumer().getAutoOffsetReset());
        // sets the serialization processing class for data keys and values.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // creating a kafka consumer instance
        return new KafkaConsumer<>(props);
    }

    /**
     * clear KafkaConsumer
     */
    public void cleanKafkaConsumer() {
        CONSUMER_MAP.clear();
        log.info("clear KafkaConsumer");
    }
}
