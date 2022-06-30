package org.opengauss.datachecker.check.service;

import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/14
 * @since ：11
 */
@Service
public class IncrementManagerService {

    @Autowired
    private FeignClientService feignClientService;

    /**
     * 增量校验日志通知
     *
     * @param dataLogList 增量校验日志
     */
    public void notifySourceIncrementDataLogs(List<SourceDataLog> dataLogList) {
        // 收集上次校验结果，并构建增量校验日志
        dataLogList.addAll(collectLastResults());

        feignClientService.notifyIncrementDataLogs(Endpoint.SOURCE, dataLogList);
        feignClientService.notifyIncrementDataLogs(Endpoint.SINK, dataLogList);
    }

    /**
     *  收集上次校验结果，并构建增量校验日志
     *
     * @return 上次校验结果解析
     */
    private List<SourceDataLog> collectLastResults() {
        List<SourceDataLog> dataLogList = new ArrayList<>();

        return dataLogList;
    }
}
