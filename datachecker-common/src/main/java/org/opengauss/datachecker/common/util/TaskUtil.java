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

/**
 * TaskUtil
 *
 * @author ：wangchao
 * @date ：Created in 2022/10/31
 * @since ：11
 */
public class TaskUtil {
    public static final int EXTRACT_MAX_ROW_COUNT = 10000;

    /**
     * Calculate the number of segmented tasks according to the total number recorded in the table
     *
     * @param tableRows Total table records
     * @return Total number of split tasks
     */
    public static int calcTaskCount(long tableRows) {
        return tableRows < EXTRACT_MAX_ROW_COUNT ? 1 : (int) (tableRows / EXTRACT_MAX_ROW_COUNT);
    }

    /**
     * Estimate the number of partition records of the verification task according to the table data volume and the total number of partitions of the data kafka Topic
     *
     * @param tableRows  table data volume
     * @param partitions partition
     * @return Estimate the number of partition records
     */
    public static int calcTablePartitionRowCount(long tableRows, int partitions) {
        return (int) (tableRows / partitions);
    }
}
