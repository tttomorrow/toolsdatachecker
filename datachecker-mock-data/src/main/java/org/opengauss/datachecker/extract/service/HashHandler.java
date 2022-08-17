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

package org.opengauss.datachecker.extract.service;

import org.opengauss.datachecker.common.util.LongHashFunctionWrapper;

/**
 * HashHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/8/2
 * @since ：11
 */
public class HashHandler {
    private static final String PRIMARY_DELIMITER = "_#_";
    private static final LongHashFunctionWrapper LONG_HASH_FUNCTION = new LongHashFunctionWrapper();

    /**
     * hash
     *
     * @param key hash key
     * @return hash result
     */
    public long xx3Hash(String key) {
        return LONG_HASH_FUNCTION.hashChars(key);
    }
}
