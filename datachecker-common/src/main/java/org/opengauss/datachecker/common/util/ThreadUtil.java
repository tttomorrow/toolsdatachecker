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
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * ThreadUtil
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/31
 * @since ：11
 */
@Slf4j
public class ThreadUtil {
    private static final int scheduled_core_pool_size = 5;
    private static final int single_core_pool_size = 1;
    private static final int single_thread_pool_deque_capacity = 100;

    public static ScheduledExecutorService SCHEDULED_THREAD_POOL =
        new ScheduledThreadPoolExecutor(scheduled_core_pool_size,
            new BasicThreadFactory.Builder().daemon(true).build());
    public static ThreadPoolExecutor SINGLE_THREAD_POOL =
        new ThreadPoolExecutor(single_core_pool_size, single_core_pool_size, 60L, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(single_thread_pool_deque_capacity), Executors.defaultThreadFactory(),
            new ThreadPoolExecutor.DiscardOldestPolicy());

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

    /**
     * The current thread sleeps for 1000 milliseconds
     */
    public static void sleepOneSecond() {
        sleep(1000);
    }

    /**
     * The current thread sleeps for 500 milliseconds
     */
    public static void sleepHalfSecond() {
        sleep(500);
    }

    /**
     * Custom thread pool construction
     *
     * @return thread pool
     */
    public static ThreadPoolExecutor newSingleThreadExecutor() {
        if (SINGLE_THREAD_POOL.isShutdown()) {
            SINGLE_THREAD_POOL =
                new ThreadPoolExecutor(single_core_pool_size, single_core_pool_size, 60L, TimeUnit.SECONDS,
                    new LinkedBlockingDeque<>(single_thread_pool_deque_capacity), Executors.defaultThreadFactory(),
                    new ThreadPoolExecutor.DiscardOldestPolicy());
        }
        return SINGLE_THREAD_POOL;
    }

    /**
     * Custom scheduled task single thread
     *
     * @return Scheduled task single thread
     */
    public static ScheduledExecutorService newSingleThreadScheduledExecutor() {
        if (SCHEDULED_THREAD_POOL.isShutdown()) {
            SCHEDULED_THREAD_POOL = new ScheduledThreadPoolExecutor(scheduled_core_pool_size,
                new BasicThreadFactory.Builder().daemon(true).build());
        }
        return SCHEDULED_THREAD_POOL;
    }
}
