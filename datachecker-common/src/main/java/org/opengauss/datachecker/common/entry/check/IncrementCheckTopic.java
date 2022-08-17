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

package org.opengauss.datachecker.common.entry.check;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * IncrementCheckTopic
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/24
 * @since ：11
 */
@Schema(name = "debezium增量校验topic信息")
@Data
@Accessors(chain = true)
public class IncrementCheckTopic {
    /**
     * Debezium incremental migration topic, debezium monitors table incremental data,
     * and uses a single topic for incremental data management
     */
    @Schema(name = "debeziumTopic")
    private String topic;

    @Schema(name = "groupId", description = "Topic grouping")
    private String groupId;

    @Schema(name = "partitions", description = "Topic partition")
    private int partitions;

    @Schema(name = "begin", description = "Topic start offset")
    private Long begin;

    @Schema(name = "end", description = "Topic end offset")
    private Long end;
}
