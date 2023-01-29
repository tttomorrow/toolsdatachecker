package org.opengauss.datachecker.common.service;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.util.SpringUtil;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.boot.SpringApplication;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author ：wangchao
 * @date ：Created in 2023/2/14
 * @since ：11
 */
@Slf4j
@Service
public class ShutdownService {
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final AtomicInteger monitor = new AtomicInteger(0);

    @Async
    public void shutdown(String message) {
        log.info("The check server will be shutdown , {} . check server exited .", message);
        isShutdown.set(true);
        while (monitor.get() > 0) {
            ThreadUtil.sleepHalfSecond();
        }
        System.exit(SpringApplication.exit(SpringUtil.getApplicationContext()));
    }

    public boolean isShutdown() {
        return isShutdown.get();
    }

    public synchronized int addMonitor() {
        return monitor.incrementAndGet();
    }

    public synchronized int releaseMonitor() {
        return monitor.decrementAndGet();
    }
}
