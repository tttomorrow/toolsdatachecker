package org.opengauss.datachecker.common.util;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

class PhaserUtilTest {

    @Test
    void testSubmit() {
        // Setup
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(16);
        executor.setMaxPoolSize(32);
        executor.setQueueCapacity(4000);
        executor.setKeepAliveSeconds(60);
        executor.setThreadNamePrefix("extract-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
        executor.initialize();

        final List<Runnable> tasks = List.of(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                Thread.sleep(1000);
            }
        });
        System.out.println("task exec start      " + LocalDateTime.now());
        // Run the test
        PhaserUtil.submit(executor, tasks, () -> {
            System.out.println("task exec completed " + LocalDateTime.now());
        });
        // Verify the results
    }
}
