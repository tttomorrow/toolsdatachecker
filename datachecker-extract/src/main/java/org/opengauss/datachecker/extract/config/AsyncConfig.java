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

package org.opengauss.datachecker.extract.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author wang chao
 * @date 2022/5/8 19:17
 * @since 11
 **/
@Configuration
public class AsyncConfig {

    @Bean("extractThreadExecutor")
    public ThreadPoolTaskExecutor doAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // Number of core threads, which is the number of threads initialized when the thread pool is created.
        executor.setCorePoolSize(Runtime.getRuntime().availableProcessors());
        // Maximum number of threads, maximum number of threads in the thread pool.
        executor.setMaxPoolSize(Runtime.getRuntime().availableProcessors());
        // Buffer queue: A queue used to buffer execution tasks.
        executor.setQueueCapacity(Integer.MAX_VALUE / 100);
        // Allow thread idle time.
        executor.setKeepAliveSeconds(60);
        // Allow Core Thread Timeout Shutdown
        executor.setAllowCoreThreadTimeOut(true);
        // Thread pool thread name prefix
        executor.setThreadNamePrefix("EXTRACT_");
        // Deny policy
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
        executor.initialize();
        return executor;
    }
}
