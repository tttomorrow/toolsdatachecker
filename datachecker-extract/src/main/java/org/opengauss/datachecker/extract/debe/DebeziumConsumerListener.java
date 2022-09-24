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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.opengauss.datachecker.common.exception.DebeziumConfigException;
import org.springframework.stereotype.Service;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * DebeziumConsumerListener
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/21
 * @since ：11
 */
@Slf4j
@Service
public class DebeziumConsumerListener {
    private static final LinkedBlockingQueue<DebeziumDataBean> DATA_LOG_QUEUE = new LinkedBlockingQueue<>();
    private final DebeziumDataHandler debeziumDataHandler = new DebeziumDataHandler();

    public void listen(ConsumerRecord<String, String> record) {
        try {
            debeziumDataHandler.handler(record.value(), DATA_LOG_QUEUE);
        } catch (DebeziumConfigException | JSONException | InterruptedException ex) {
            // Abnormal message structure, ignoring the current message
            log.error("Abnormal message structure, ignoring the current message,{},{}", record.value(),
                ex.getMessage());
        }
    }

    public int size() {
        return DATA_LOG_QUEUE.size();
    }

    public DebeziumDataBean poll() {
        return DATA_LOG_QUEUE.poll();
    }
}
