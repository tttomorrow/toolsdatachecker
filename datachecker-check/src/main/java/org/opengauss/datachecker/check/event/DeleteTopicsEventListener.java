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

package org.opengauss.datachecker.check.event;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author ：wangchao
 * @date ：Created in 2023/3/7
 * @since ：11
 */
@Slf4j
@Component
public class DeleteTopicsEventListener implements ApplicationListener<DeleteTopicsEvent> {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    private AdminClient adminClient = null;

    @Override
    public void onApplicationEvent(DeleteTopicsEvent event) {
        log.debug("delete topic event listener : {}", event.getMessage());
        final Object source = event.getSource();
        initAdminClient();
        deleteTopic((DeleteTopics) source);
    }

    private void deleteTopic(DeleteTopics deleteOption) {
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(deleteOption.getTopicList());
        final KafkaFuture<Void> kafkaFuture = deleteTopicsResult.all();
        try {
            kafkaFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("delete topic event error : ", e);
        }
    }

    private void initAdminClient() {
        if (this.adminClient == null) {
            Map<String, Object> props = new HashMap<>(1);
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            this.adminClient = KafkaAdminClient.create(props);
        }
    }
}
