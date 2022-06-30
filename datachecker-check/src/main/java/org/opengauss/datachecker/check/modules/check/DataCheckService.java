package org.opengauss.datachecker.check.modules.check;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.config.DataCheckConfig;
import org.opengauss.datachecker.common.entry.check.DataCheckParam;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Slf4j
@Service
public class DataCheckService {

    @Autowired
    private FeignClientService feignClientService;

    @Autowired
    private DataCheckConfig dataCheckConfig;

    @Autowired
    @Qualifier("asyncCheckExecutor")
    private ThreadPoolTaskExecutor checkAsyncExecutor;

    /**
     * @param topic
     * @param partitions
     */
    public void checkTableData(@NonNull Topic topic, int partitions) {
        final int bucketCapacity = dataCheckConfig.getBucketCapacity();
        final String checkResultPath = dataCheckConfig.getCheckResultPath();

        String schema = feignClientService.getDatabaseSchema(Endpoint.SINK);
        final DataCheckParam checkParam = new DataCheckParam(bucketCapacity, topic, partitions, checkResultPath, schema);
        checkAsyncExecutor.submit(new DataCheckThread(checkParam, feignClientService));
    }

    public void incrementCheckTableData(Topic topic) {
        final int bucketCapacity = dataCheckConfig.getBucketCapacity();
        final String checkResultPath = dataCheckConfig.getCheckResultPath();
        String schema = feignClientService.getDatabaseSchema(Endpoint.SINK);
        final DataCheckParam checkParam = new DataCheckParam(bucketCapacity, topic, 0, checkResultPath, schema);
        checkAsyncExecutor.submit(new IncrementDataCheckThread(checkParam, feignClientService));
    }
}
