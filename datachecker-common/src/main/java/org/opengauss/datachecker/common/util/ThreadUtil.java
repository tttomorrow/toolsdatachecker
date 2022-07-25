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
     * 线程休眠
     *
     * @param millisTime 休眠时间毫秒
     */
    public static void sleep(int millisTime) {
        try {
            Thread.sleep(millisTime);
        } catch (InterruptedException ie) {
            log.error("thread sleep interrupted exception ");
        }
    }

    public static ThreadPoolExecutor newSingleThreadExecutor() {
        return new ThreadPoolExecutor(1, 1, 60L, TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(100),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.DiscardOldestPolicy());

    }
}
