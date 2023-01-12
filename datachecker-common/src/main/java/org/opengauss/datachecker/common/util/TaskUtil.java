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

import java.util.stream.IntStream;

/**
 * TaskUtil
 *
 * @author ：wangchao
 * @date ：Created in 2022/10/31
 * @since ：11
 */
public class TaskUtil {
    public static final int EXTRACT_MAX_ROW_COUNT = 50000;
    private static final int[] MAX_LIMIT =
        {50000, 100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000};
    private static final float CPU_UTILIZATION = 2.0f;

    private static int calcMaxLimitRowCount(long tableRows) {
        if (tableRows < MAX_LIMIT[0]) {
            return MAX_LIMIT[0];
        }
        final int processors = Runtime.getRuntime().availableProcessors();
        int maxLimitRowCount = (int) (tableRows * 10 / (processors * CPU_UTILIZATION * 10));
        int level = 0;
        int i = 0;
        for (; i < MAX_LIMIT.length; i++) {
            if (MAX_LIMIT[i] > maxLimitRowCount) {
                maxLimitRowCount = MAX_LIMIT[i];
                level = i;
                break;
            }
            i++;
        }
        if (level != i) {
            maxLimitRowCount = MAX_LIMIT[MAX_LIMIT.length - 1];
        }
        return maxLimitRowCount;
    }

    public static int calcAutoTaskCount(long tableRows) {
        if (tableRows < MAX_LIMIT[0]) {
            return 1;
        }
        int maxLimitRowCount = calcMaxLimitRowCount(tableRows);
        final int taskCount = (int) Math.round(tableRows * 1.0 / maxLimitRowCount);
        return taskCount;
    }

    /**
     * Calculate the number of segmented tasks according to the total number recorded in the table
     *
     * @param tableRows Total table records
     * @return Total number of split tasks offset
     */
    public static int[][] calcAutoTaskOffset(long tableRows) {
        if (tableRows < MAX_LIMIT[0]) {
            return new int[][] {{0, MAX_LIMIT[0]}};
        }
        int maxLimitRowCount = calcMaxLimitRowCount(tableRows);
        final int taskCount = (int) Math.round(tableRows * 1.0 / maxLimitRowCount);
        final int lastTaskRowCount = maxLimitRowCount + (int) (tableRows % maxLimitRowCount);
        int[][] taskOffset = new int[taskCount][2];
        IntStream.range(0, taskCount).forEach(taskCountIdx -> {
            int start = taskCountIdx * maxLimitRowCount;
            int size = taskCount == taskCountIdx + 1 ? lastTaskRowCount : maxLimitRowCount;
            taskOffset[taskCountIdx] = new int[] {start, size};
        });
        return taskOffset;
    }

    /**
     * Estimate the number of partition records of the verification task according to the table data volume and the total number of partitions of the data kafka Topic
     *
     * @param tableRows  table data volume
     * @param partitions partition
     * @return Estimate the number of partition records
     */
    public static int calcTablePartitionRowCount(long tableRows, int partitions) {
        if (partitions == 0) {
            partitions = 1;
        }
        return (int) (tableRows / partitions);
    }
}
