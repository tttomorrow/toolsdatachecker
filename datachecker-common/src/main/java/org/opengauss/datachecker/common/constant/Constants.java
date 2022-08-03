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

package org.opengauss.datachecker.common.constant;

/**
 * Constants
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/24
 * @since ：11
 */
public interface Constants {
    /**
     * Combined primary key splice connector
     */
    String PRIMARY_DELIMITER = "_#_";

    /**
     * DELIMITER
     */
    String DELIMITER = ",";

    interface InitialCapacity {
        /**
         * map initial capacity
         */
        int MAP = 0;
    }
}
