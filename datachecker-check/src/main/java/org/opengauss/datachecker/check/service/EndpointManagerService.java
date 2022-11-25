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

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.config.DataCheckProperties;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Data Extraction Service Endpoint Management
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/26
 * @since ：11
 */
@Slf4j
@Service
public class EndpointManagerService {
    @Autowired
    private FeignClientService feignClientService;
    @Autowired
    private DataCheckProperties dataCheckProperties;
    @Autowired
    private EndpointStatusManager endpointStatusManager;

    /**
     * View the health status of all endpoints
     *
     * @return health status
     */
    public boolean isEndpointHealth() {
        return endpointStatusManager.isEndpointHealth();
    }

    /**
     * Endpoint health check
     */
    @Scheduled(cron = "0/5 * * * * ?")
    public void endpointHealthCheck() {
        Thread.currentThread().setName("heart-beat-heath-" + Thread.currentThread().getId());
        checkEndpoint(dataCheckProperties.getSourceUri(), Endpoint.SOURCE, "source endpoint service check");
        checkEndpoint(dataCheckProperties.getSinkUri(), Endpoint.SINK, "sink endpoint service check");
    }

    private void checkEndpoint(String requestUri, Endpoint endpoint, String message) {
        // service network check ping
        try {
            // service check: service database check
            Result healthStatus = feignClientService.getClient(endpoint).health();
            if (healthStatus.isSuccess()) {
                endpointStatusManager.resetStatus(endpoint, Boolean.TRUE);
                log.debug("{}：{} current state health", message, requestUri);
            } else {
                endpointStatusManager.resetStatus(endpoint, Boolean.FALSE);
                log.error("{}:{} current service status is abnormal", message, requestUri);
            }
        } catch (Exception ce) {
            log.error("{}:{} service unreachable", message, ce.getMessage());
            endpointStatusManager.resetStatus(endpoint, Boolean.FALSE);
        }
    }
}
