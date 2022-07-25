package org.opengauss.datachecker.extract.service;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.extract.service.thread.ExtractMockDataThread;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.util.concurrent.Future;

@Slf4j
@Service
public class ExtractMockDataService {

    /**
     * 限制最大线程总数
     */
    private static final int MAX_THREAD_COUNT = 100;

    @Autowired
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    @Autowired
    private DataSource dataSourceOne;

    /**
     * 向指定表名称，采用多线程方式批量插入指定数据量的Mock数据
     *
     * @param tableName   待插入数据的表名称
     * @param totalCount  插入记录总数
     * @param threadCount 插入记录线程总数
     */
    public void batchMockData(String tableName, long totalCount, int threadCount) {
        try {
            Assert.isTrue(threadCount < MAX_THREAD_COUNT, "设置的线程总数不能超过最大线程总数");
            long batchCount = totalCount / threadCount;

            log.info("plan batch insert thread, tableName = {}, threadCount = {} ,totalCount = {} , batchCount = {}", tableName, threadCount, totalCount, batchCount);
            for (int i = 0; i < threadCount; i++) {
                if (i == (threadCount - 1)) {
                    batchCount = batchCount + totalCount % threadCount;
                }
                threadPoolTaskExecutor.submit(new ExtractMockDataThread(dataSourceOne, tableName, batchCount, i + 1));

            }
        } catch (Exception throwables) {
            log.error("=============", throwables.getMessage());
        }

    }
}
