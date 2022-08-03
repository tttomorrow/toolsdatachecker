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
     * Incremental verification log notification
     *
     * @param dataLogList Incremental verification log
     */
    public void notifySourceIncrementDataLogs(List<SourceDataLog> dataLogList) {
        // Collect the last verification results and build an incremental verification log
        dataLogList.addAll(collectLastResults());

        feignClientService.notifyIncrementDataLogs(Endpoint.SOURCE, dataLogList);
        feignClientService.notifyIncrementDataLogs(Endpoint.SINK, dataLogList);
    }

    /**
     * Collect the last verification results and build an incremental verification log
     *
     * @return Analysis of last verification result
     */
    private List<SourceDataLog> collectLastResults() {
        List<SourceDataLog> dataLogList = new ArrayList<>();

        return dataLogList;
    }
}
