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
        executor.setCorePoolSize(Runtime.getRuntime().availableProcessors() * 2);
        // Maximum number of threads, maximum number of threads in the thread pool.
        executor.setMaxPoolSize(Runtime.getRuntime().availableProcessors() * 4);
        // Buffer queue: A queue used to buffer execution tasks.
        executor.setQueueCapacity(Integer.MAX_VALUE);
        // Allow thread idle time.
        executor.setKeepAliveSeconds(60);
        // Allow Core Thread Timeout Shutdown
        executor.setAllowCoreThreadTimeOut(true);
        // Thread pool thread name prefix
        executor.setThreadNamePrefix("extract-thread");
        // Deny policy
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
        executor.initialize();
        return executor;
    }
}
