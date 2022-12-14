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

package org.opengauss.datachecker.common.util;

import org.opengauss.datachecker.common.entry.enums.Endpoint;

/**
 * TopicUtil
 *
 * @author ：wangchao
 * @date ：Created in 2022/10/31
 * @since ：11
 */
public class TopicUtil {
    public static final int TOPIC_MAX_PARTITIONS = 16;
    public static final int TOPIC_MIN_PARTITIONS = 1;

    private static final String TOPIC_TEMPLATE = "CHECK_%s_%s_%s";
    private static final String UPPER_CODE = "1";
    private static final String LOWER_CODE = "0";

    /**
     * buildTopicName
     *
     * @param process   process
     * @param endpoint  endpoint
     * @param tableName tableName
     * @return topicName
     */
    public static String buildTopicName(String process, Endpoint endpoint, String tableName) {
        return String.format(TOPIC_TEMPLATE, process, endpoint.getCode(), tableName) + letterCaseEncoding(tableName);
    }

    /**
     * Calculate the Kafka partition according to the total number of task slices.
     * The total number of Kafka partitions shall not exceed {@value TOPIC_MAX_PARTITIONS}
     *
     * @param divisions Number of task slices extracted
     * @return Total number of Kafka partitions
     */
    public static int calcPartitions(int divisions) {
        if (divisions <= 1) {
            return TOPIC_MIN_PARTITIONS;
        }
        final int partitions = divisions / 2;
        return Math.min(partitions, TOPIC_MAX_PARTITIONS);
    }

    private static String letterCaseEncoding(String tableName) {
        final char[] chars = tableName.toCharArray();
        StringBuilder builder = new StringBuilder("0");
        for (char aChar : chars) {
            if (aChar >= 'A' && aChar <= 'Z') {
                builder.append(UPPER_CODE);
            } else if (aChar >= 'a' && aChar <= 'z') {
                builder.append(LOWER_CODE);
            }
        }
        final String encoding = builder.toString();
        return Long.toHexString(Long.valueOf(encoding,2));
    }
}
