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

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.debezium.DebePayload;
import org.opengauss.datachecker.common.entry.debezium.DebeziumData;
import org.opengauss.datachecker.common.entry.debezium.PayloadSource;
import org.springframework.stereotype.Service;

import javax.validation.constraints.NotNull;
import java.util.Map;

/**
 * DebeziumDataHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/24
 * @since ：11
 */
@Slf4j
@Service
public class DebeziumDataHandler {
    /**
     * Debezium message parsing and adding the parsing result to the {@code DebeziumDataLogs.class} result set
     *
     * @param message          message
     * @param debeziumDataLogs debeziumDataLogs
     */
    public void handler(String message, @NotNull DebeziumDataLogs debeziumDataLogs) {
        final DebeziumData debeziumData = JSONObject.parseObject(message, DebeziumData.class);
        final DebePayload payload = debeziumData.getPayload();
        final Map<String, String> before = payload.getBefore();
        final Map<String, String> after = payload.getAfter();
        final PayloadSource source = payload.getSource();
        // Extract the data and add it to the debezium incremental log object
        if (!debeziumDataLogs.addDebeziumDataKey(source.getTable(), after != null ? after : before)) {
            // The format of the debezium message is abnormal.
            // The corresponding table [{}] of the current message does not exist or is illegal
            log.error("The debezium message format is abnormal. The current message corresponding table [{}] "
                + "does not exist or is illegal", source.getTable());
        }
    }
}
