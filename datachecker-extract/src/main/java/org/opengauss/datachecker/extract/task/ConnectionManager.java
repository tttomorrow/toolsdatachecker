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

package org.opengauss.datachecker.extract.task;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.extract.config.DruidDataSourceConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author ：wangchao
 * @date ：Created in 2023/3/25
 * @since ：11
 */
@Slf4j
@Service
@ConditionalOnBean(DruidDataSourceConfig.class)
public class ConnectionManager {
    private ReentrantLock lock = new ReentrantLock();
    @Value("${spring.extract.query-dop}")
    private int queryDop;
    @Resource
    private DruidDataSourceConfig dataSourceConfig;

    private volatile AtomicInteger connectionCount = new AtomicInteger(0);

    @PostConstruct
    public void initMaxConnectionCount() {
        final DruidDataSource dataSource = (DruidDataSource) dataSourceConfig.druidDataSourceOne();
        connectionCount.set(dataSource.getMaxActive());
        log.info("max active connection {}", connectionCount.get());
    }

    public int getParallelQueryDop() {
        return queryDop;
    }

    public boolean getConnection() {
        lock.lock();
        try {
            if (connectionCount.get() > 2) {
                connectionCount.decrementAndGet();
                return true;
            }
        } finally {
            lock.unlock();
        }
        return false;
    }

    public void releaseConnection() {
        lock.lock();
        try {
            connectionCount.incrementAndGet();
        } finally {
            lock.unlock();
        }
    }
}
