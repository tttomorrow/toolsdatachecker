package org.opengauss.datachecker.extract.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/17
 * @since ：11
 */
@Slf4j
@Component
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaProducerConfig {

    private static final Object LOCK = new Object();
    private static final Map<String, KafkaProducer<String, String>> PRODUCER_MAP = new ConcurrentHashMap<>();

    @Autowired
    private KafkaProperties properties;

    /**
     *Obtaining a specified producer client based on topic.
     *
     * @param topic topic name
     * @return the topic corresponds to the producer client.
     */
    public KafkaProducer<String, String> getKafkaProducer(String topic) {
        KafkaProducer<String, String> producer = PRODUCER_MAP.get(topic);
        if (Objects.isNull(producer)) {
            synchronized (LOCK) {
                producer = PRODUCER_MAP.get(topic);
                if (Objects.isNull(producer)) {
                    producer = buildKafkaProducer();
                    PRODUCER_MAP.put(topic, producer);
                }
            }
        }
        return producer;
    }

    private KafkaProducer<String, String> buildKafkaProducer() {
        // configuration information
        Properties props = new Properties();
        // kafka server address
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers().stream().collect(Collectors.joining(",")));
        props.put(ProducerConfig.ACKS_CONFIG, properties.getProducer().getAcks());
        // sets the serialization processing class for data keys and values.
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, properties.getProducer().getKeySerializer());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, properties.getProducer().getValueSerializer());
        // creating a kafka producer instance
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }

    public KafkaProducer<String, String> getDebeziumKafkaProducer() {
        return buildKafkaProducer();
    }

    /**
     * clear KafkaProducer
     */
    public void cleanKafkaProducer() {
        PRODUCER_MAP.clear();
        log.info("clear KafkaProducer");
    }
}
