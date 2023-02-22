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

package org.opengauss.datachecker.extract.debezium;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * DebeziumAvroHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/24
 * @since ：11
 */
@Slf4j
public class DebeziumAvroHandler implements DebeziumDataHandler<GenericData.Record> {
    private static final String AVRO_FIELD_BEFORE = "before";
    private static final String AVRO_FIELD_AFTER = "after";
    private static final String AVRO_FIELD_SOURCE = "source";
    private static final String AVRO_FIELD_TABLE = "table";

    /**
     * Debezium message parsing and adding the parsing result to the {@code DebeziumDataLogs.class} result set
     *
     * @param offset  offset
     * @param message message
     * @param queue   debeziumDataLogs
     */
    @Override
    public void handler(long offset, @NotEmpty GenericData.Record message,
        @NotNull LinkedBlockingQueue<DebeziumDataBean> queue) {
        try {
            final Map<String, String> before = parseRecordData(message, AVRO_FIELD_BEFORE);
            final Map<String, String> after = parseRecordData(message, AVRO_FIELD_AFTER);
            final Map<String, String> source = parseRecordData(message, AVRO_FIELD_SOURCE);
            if (source.containsKey(AVRO_FIELD_TABLE)) {
                queue.put(new DebeziumDataBean(source.get(AVRO_FIELD_TABLE), offset, after.isEmpty() ? after : before));
            }
        } catch (InterruptedException ex) {
            log.error("put message at the tail of this queue, waiting if necessary for space to become available.");
        }
    }

    private Map<String, String> parseRecordData(Record message, String key) {
        if (Objects.nonNull(message.get(key))) {
            return JSONObject.parseObject(message.get(key).toString(), new TypeReference<>() {});
        } else {
            return new HashMap<>(0);
        }
    }
}
