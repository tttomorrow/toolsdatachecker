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
import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.config.DataCheckProperties;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 数据抽取服务端点管理
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/26
 * @since ：11
 */
@Slf4j
@Service
public class EndpointManagerService {
    private static final String ENDPOINT_HEALTH_CHECK_THREAD_NAME = "endpoint-health-check-thread";
    private static final ScheduledExecutorService SCHEDULED_EXECUTOR = Executors.newSingleThreadScheduledExecutor();

    @Autowired
    private FeignClientService feignClientService;
    @Autowired
    private DataCheckProperties dataCheckProperties;
    @Autowired
    private EndpointStatusManager endpointStatusManager;

    /**
     * Start the health check self check thread
     */
    public void start() {
        endpointHealthCheck();
        SCHEDULED_EXECUTOR.scheduleWithFixedDelay(() -> {
            Thread.currentThread().setName(ENDPOINT_HEALTH_CHECK_THREAD_NAME);
            endpointHealthCheck();
        }, 0, 2, TimeUnit.SECONDS);
    }

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
    public void endpointHealthCheck() {
        checkEndpoint(dataCheckProperties.getSourceUri(), Endpoint.SOURCE, "source endpoint service check");
        checkEndpoint(dataCheckProperties.getSinkUri(), Endpoint.SINK, "sink endpoint service check");
    }

    private void checkEndpoint(String requestUri, Endpoint endpoint, String message) {
        // service network check ping
        try {
            if (NetworkCheck.networkCheck(getEndpointIp(requestUri))) {

                // service check: service database check
                Result healthStatus = feignClientService.getClient(endpoint).health();
                if (healthStatus.isSuccess()) {
                    endpointStatusManager.resetStatus(endpoint, Boolean.TRUE);
                    log.debug("{}：{} current state health", message, requestUri);
                } else {
                    endpointStatusManager.resetStatus(endpoint, Boolean.FALSE);
                    log.error("{}:{} current service status is abnormal", message, requestUri);
                }
            }
        } catch (Exception ce) {
            log.error("{}:{} service unreachable", message, ce.getMessage());
            endpointStatusManager.resetStatus(endpoint, Boolean.FALSE);
        }
    }

    /**
     * Resolve the corresponding IP address according to the endpoint URI address in the configuration attribute
     * uri address: http://127.0.0.1:8080     https://127.0.0.1:8080
     *
     * @param endpointUri Configure the endpoint URI in the attribute
     * @return If the resolution is successful, the corresponding IP address is returned; otherwise, null is returned
     */
    private String getEndpointIp(String endpointUri) {
        if (checkLegalOfHttpProtocol(endpointUri) && checkLegalOfIp(endpointUri) && checkLegalOfPort(endpointUri)) {
            return endpointUri.replace(NetAddress.IP_DELIMITER, NetAddress.PORT_DELIMITER)
                              .split(NetAddress.PORT_DELIMITER)[1];
        }
        return null;
    }

    private boolean checkLegalOfPort(String endpointUri) {
        return checkLegalOfUri(endpointUri, NetAddress.PORT_DELIMITER);
    }

    private boolean checkLegalOfIp(String endpointUri) {
        return checkLegalOfUri(endpointUri, NetAddress.IP_DELIMITER);
    }

    private boolean checkLegalOfUri(String endpointUri, String ipDelemter) {
        return endpointUri.contains(ipDelemter);
    }

    private boolean checkLegalOfHttpProtocol(String endpointUri) {
        return checkLegalOfUri(endpointUri, NetAddress.HTTP) || checkLegalOfUri(endpointUri, NetAddress.HTTPS);
    }

    /**
     * Close the self check thread of jiangkang
     */
    public void shutdown() {
        SCHEDULED_EXECUTOR.shutdownNow();
    }

    interface NetAddress {
        /**
         * http
         */
        String HTTP = "http";

        /**
         * https
         */
        String HTTPS = "https";

        /**
         * ip delimiter
         */
        String IP_DELIMITER = "://";

        /**
         * port delimiter
         */
        String PORT_DELIMITER = ":";
    }

    /**
     * Network status check
     */
    static class NetworkCheck {
        private static final String PING = "ping ";
        private static final String TTL = "TTL";

        /**
         * Check the network status according to the system command Ping {@code ip}
         *
         * @param ip ip address
         * @return Network check results
         */
        public static boolean networkCheck(String ip) {
            boolean result = false;
            if (StringUtils.isEmpty(ip)) {
                log.error("network check error : ip addr is null");
                return result;
            }

            String line;
            String endMsg = null;
            StringBuffer sb = new StringBuffer();
            String cmd = PING + ip;
            try {
                Process process = Runtime.getRuntime().exec(cmd);
                try (BufferedReader buffer = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), Charset.forName("GBK")))) {
                    while ((line = buffer.readLine()) != null) {
                        sb.append(line);
                        endMsg = line;
                    }
                    if (StringUtils.contains(sb.toString(), TTL)) {
                        result = true;
                        log.debug("ip {} network check normal", cmd);
                    } else {
                        log.error("ip {} network check error : {}", cmd, endMsg);
                    }
                } catch (IOException io) {
                    throw new IOException("read process result bufferedReader error");
                }
            } catch (IOException io) {
                log.error("ip {} network check error : {} ", ip, io.getMessage());
            }
            return result;
        }
    }
}
