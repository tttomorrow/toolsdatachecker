package org.opengauss.datachecker.extract.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author wang chao
 * @date 2022/5/8 19:17
 * @since 11
 **/
@Configuration
@EnableAsync
public class AsyncConfig {

    @Bean("threadPoolTaskExecutor")
    public ThreadPoolTaskExecutor doAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // 核心线程数， 当前机器的核心数 线程池创建时初始化线程数量
        executor.setCorePoolSize(16);
        // 最大线程数：线程池最大的线程数，只有在缓冲队列满了之后才会申请超过核心线程数的线程
        executor.setMaxPoolSize(32);
        // 缓冲队列： 用来缓冲执行任务的队列
        executor.setQueueCapacity(4000);
        //允许线程空闲时间
        executor.setKeepAliveSeconds(60);
        // 线程池名称前缀
        executor.setThreadNamePrefix("extract-");
        // 缓冲队列满了之后的拒绝策略：
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
        executor.initialize();
        return executor;
    }
}
