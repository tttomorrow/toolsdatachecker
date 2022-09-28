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

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opengauss.datachecker.extract.config.KafkaConsumerConfig;

import java.time.Duration;

/**
 * DebeziumWorker
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/21
 * @since ：11
 */
@Slf4j
public class DebeziumWorker implements Runnable {
    private static final String NAME = "DebeziumWorker";
    private DebeziumConsumerListener debeziumConsumerListener;
    private KafkaConsumerConfig kafkaConsumerConfig;

    public DebeziumWorker(DebeziumConsumerListener debeziumConsumerListener, KafkaConsumerConfig kafkaConsumerConfig) {
        this.debeziumConsumerListener = debeziumConsumerListener;
        this.kafkaConsumerConfig = kafkaConsumerConfig;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(NAME);
        log.info("The Debezium message listener task has started");
        final KafkaConsumer<String, String> consumer = kafkaConsumerConfig.getDebeziumConsumer();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(50));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    debeziumConsumerListener.listen(record);
                } catch (Exception ex) {
                    log.error("DebeziumWorker Abnormal message, ignoring the current message,{},{}", record.toString(),
                        ex);
                }
            }
        }
    }
}
