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

package org.opengauss.datachecker.common.thread;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ThreadPoolFactory
 *
 * @author ：wangchao
 * @date ：Created in 2022/11/17
 * @since ：11
 */
@Slf4j
public class ThreadPoolFactory {
    private static final double TARGET_UTILIZATION = 0.7;
    private static final double IO_WAIT_TIME = 4.0;
    private static final double CPU_TIME = 1.0;
    private static final double POOL_QUEUE_EXPANSION_RATIO = 1.2;
    private static final double CHECK_POOL_QUEUE_EXPANSION_RATIO = 0.5;
    private static final double CORE_POOL_SIZE_RATIO = 2.0;

    /**
     * Initialize the extract service thread pool
     *
     * @param threadName threadName
     * @param threadSize threadSize
     * @return ExecutorService
     */
    public static ExecutorService newThreadPool(String threadName, int threadSize) {
        return createThreadPool(threadName, threadSize);
    }

    /**
     * Initialize the verification service thread pool
     *
     * @param threadName threadName
     * @param threadSize threadSize
     * @return ExecutorService
     */
    public static ExecutorService newCheckThreadPool(String threadName, int threadSize) {
        return createCheckThreadPool(threadName, threadSize);
    }

    private static ExecutorService createCheckThreadPool(String threadName, int size) {
        int queueSize = calculateCheckQueueCapacity(size);
        int threadNum = calculateOptimalThreadCount(CPU_TIME, IO_WAIT_TIME, TARGET_UTILIZATION);
        int corePoolSize = calculateCorePoolSize(threadNum);
        return createThreadPool(threadName, corePoolSize, threadNum, queueSize);
    }

    private static ExecutorService createThreadPool(String threadName, int size) {
        int queueSize = calculateCheckQueueCapacity(size);
        int threadNum = calculateOptimalThreadCount(CPU_TIME, IO_WAIT_TIME, TARGET_UTILIZATION);
        return createThreadPool(threadName, threadNum, threadNum, queueSize);
    }

    private static ExecutorService createThreadPool(String threadName, int corePoolSize, int threadNum, int queueSize) {

        log.info("Thread name is {}, corePoolSize is : {}, size is {}, queueSize is {}", threadName, corePoolSize,
            threadNum, queueSize);
        BlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<>(queueSize);

        ThreadPoolExecutor threadPoolExecutor =
            new ThreadPoolExecutor(corePoolSize, threadNum, 60L, TimeUnit.SECONDS, blockingQueue,
                new CheckThreadFactory("check", threadName, false), new DiscardOldestPolicy(log, threadName));
        threadPoolExecutor.allowCoreThreadTimeOut(true);
        return threadPoolExecutor;
    }

    private static int calculateQueueCapacity(int size) {
        return (int) Math.ceil(size * POOL_QUEUE_EXPANSION_RATIO);
    }

    private static int calculateCheckQueueCapacity(int size) {
        return (int) Math.ceil(size * CHECK_POOL_QUEUE_EXPANSION_RATIO);
    }

    private static int calculateCorePoolSize(int threadNum) {
        return (int) Math.ceil(threadNum / CORE_POOL_SIZE_RATIO);
    }

    private static int calculateOptimalThreadCount(double computeTime, double waitTime, double targetUtilization) {
        int numberOfCpu = Runtime.getRuntime().availableProcessors();
        return (int) Math.ceil(numberOfCpu * targetUtilization * (Math.round(waitTime / computeTime) + 1));
    }

    public static class CheckThreadFactory implements ThreadFactory {
        private static final ConcurrentHashMap<String, ThreadGroup> THREAD_GROUPS = new ConcurrentHashMap<>();

        private static final AtomicInteger POOL_COUNTER = new AtomicInteger(0);

        private final AtomicLong counter = new AtomicLong(0L);

        private final int poolId;

        private final ThreadGroup group;

        private final String prefix;

        private final boolean daemon;

        public CheckThreadFactory(String groupName, String prefix, boolean daemon) {
            this.poolId = POOL_COUNTER.incrementAndGet();
            this.prefix = prefix;
            this.daemon = daemon;
            this.group = this.initThreadGroup(groupName);
        }

        @Override
        public Thread newThread(Runnable r) {
            String trName = String.format("Pool-%d-%s-%d", this.poolId, this.prefix, this.counter.incrementAndGet());
            Thread thread = new Thread(this.group, r);
            thread.setName(trName);
            thread.setDaemon(this.daemon);
            thread.setUncaughtExceptionHandler(new CheckUncaughtExceptionHandler(log));
            return thread;
        }

        private ThreadGroup initThreadGroup(String groupName) {
            if (THREAD_GROUPS.get(groupName) == null) {
                THREAD_GROUPS.putIfAbsent(groupName, new ThreadGroup(groupName));
            }
            return THREAD_GROUPS.get(groupName);
        }
    }
}
