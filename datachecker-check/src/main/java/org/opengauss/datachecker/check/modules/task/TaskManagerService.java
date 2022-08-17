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

package org.opengauss.datachecker.check.modules.task;

import org.opengauss.datachecker.common.entry.enums.Endpoint;

import java.util.List;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
public interface TaskManagerService {
    /**
     * Refresh the execution status of the data extraction table of the specified task
     *
     * @param tableName tableName
     * @param endpoint  endpoint {@link org.opengauss.datachecker.common.entry.enums.Endpoint}
     */
    void refreshTableExtractStatus(String tableName, Endpoint endpoint);

    /**
     * Initialize task status
     *
     * @param tableNameList table name list
     */
    void initTableExtractStatus(List<String> tableNameList);

    /**
     * Clean up task status information
     */
    void cleanTaskStatus();
}
