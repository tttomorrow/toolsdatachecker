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

package org.opengauss.datachecker.check.modules.check;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.check.config.DataCheckConfig;
import org.opengauss.datachecker.common.entry.check.DataCheckParam;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.concurrent.Future;

/**
 * DataCheckService
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Slf4j
@Service
public class DataCheckService {
    @Autowired
    private KafkaProperties kafkaProperties;
    @Autowired
    private DataCheckRunnableSupport dataCheckRunnableSupport;
    @Autowired
    private DataCheckConfig dataCheckConfig;
    @Autowired
    @Qualifier("asyncCheckExecutor")
    private ThreadPoolTaskExecutor checkAsyncExecutor;

    /**
     * submit check table data runnable
     *
     * @param topic      topic
     * @param partitions partitions
     * @return future
     */
    public Future<?> checkTableData(@NonNull Topic topic, int partitions) {
        DataCheckParam checkParam = buildCheckParam(topic, partitions, dataCheckConfig);
        final DataCheckRunnable dataCheckRunnable = new DataCheckRunnable(checkParam, dataCheckRunnableSupport);
        return checkAsyncExecutor.submit(dataCheckRunnable);
    }

    private DataCheckParam buildCheckParam(Topic topic, int partitions, DataCheckConfig dataCheckConfig) {
        final int bucketCapacity = dataCheckConfig.getBucketCapacity();
        final int errorRate = dataCheckConfig.getDataCheckProperties().getErrorRate();
        final String checkResultPath = dataCheckConfig.getCheckResultPath();
        return new DataCheckParam().setBucketCapacity(bucketCapacity).setTopic(topic).setPartitions(partitions)
                .setProperties(kafkaProperties).setPath(checkResultPath).setErrorRate(errorRate);
    }

    /**
     * incrementCheckTableData
     *
     * @param tableName      tableName
     * @param checkDataCount
     */
    public void incrementCheckTableData(String tableName, int checkDataCount) {
        DataCheckParam checkParam = buildIncrementCheckParam(tableName, dataCheckConfig);
        final IncrementCheckThread incrementCheck =
            new IncrementCheckThread(checkParam, checkDataCount, dataCheckRunnableSupport);
        checkAsyncExecutor.submit(incrementCheck);
    }

    private DataCheckParam buildIncrementCheckParam(String tableName, DataCheckConfig dataCheckConfig) {
        final int bucketCapacity = dataCheckConfig.getBucketCapacity();
        final String checkResultPath = dataCheckConfig.getCheckResultPath();
        return new DataCheckParam().setTableName(tableName).setBucketCapacity(bucketCapacity).setPartitions(0)
                .setPath(checkResultPath);
    }
}
