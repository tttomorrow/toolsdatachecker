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

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/31
 * @since ：11
 */
@Slf4j
public class ThreadUtil {
    /**
     * Thread hibernation
     *
     * @param millisTime Sleep time MS
     */
    public static void sleep(int millisTime) {
        try {
            Thread.sleep(millisTime);
        } catch (InterruptedException ie) {
            log.error("thread sleep interrupted exception ");
        }
    }

    public static ThreadPoolExecutor newSingleThreadExecutor() {
        return new ThreadPoolExecutor(1, 1, 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<>(100),
            Executors.defaultThreadFactory(), new ThreadPoolExecutor.DiscardOldestPolicy());
    }
}
