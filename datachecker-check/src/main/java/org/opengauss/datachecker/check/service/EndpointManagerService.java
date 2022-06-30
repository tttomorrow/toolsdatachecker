package org.opengauss.datachecker.check.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.config.DataCheckProperties;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.concurrent.*;

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
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    @Autowired
    private FeignClientService feignClientService;

    @Autowired
    private DataCheckProperties dataCheckProperties;

    @PostConstruct
    public void start() {
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            Thread.currentThread().setName(ENDPOINT_HEALTH_CHECK_THREAD_NAME);
            endpointHealthCheck();
        }, 0, 5, TimeUnit.SECONDS);
    }

    public void endpointHealthCheck() {
        checkEndpoint(dataCheckProperties.getSourceUri(), Endpoint.SOURCE, "源端服务检查");
        checkEndpoint(dataCheckProperties.getSinkUri(), Endpoint.SINK, "目标端服务检查");
    }

    private void checkEndpoint(String requestUri, Endpoint endpoint, String message) {
        // 服务网络检查ping
        try {
            if (NetworkCheck.networkCheck(getEndpointIp(requestUri))) {

                // 服务检查  服务数据库检查
                Result healthStatus = feignClientService.getClient(endpoint).health();
                if (healthStatus.isSuccess()) {
                    log.debug("{}：{} current state health", message, requestUri);
                } else {
                    log.error("{}:{} current service status is abnormal", message, requestUri);
                }

            }
        } catch (Exception ce) {
            log.error("{}:{} service unreachable", message, ce.getMessage());
        }
    }

    /**
     * 根据配置属性中的端点URI地址，解析对应的IP地址
     * URI地址: http://127.0.0.1:8080   https://127.0.0.1:8080
     *
     * @param endpointUri 配置属性中的端点URI
     * @return 若解析成功，则返回对应IP地址，否则返回null
     */
    private String getEndpointIp(String endpointUri) {
        if ((endpointUri.contains(NetAddress.HTTP) || endpointUri.contains(NetAddress.HTTPS))
                && endpointUri.contains(NetAddress.IP_DELEMTER) && endpointUri.contains(NetAddress.PORT_DELEMTER)) {
            return endpointUri.replace(NetAddress.IP_DELEMTER, NetAddress.PORT_DELEMTER).split(NetAddress.PORT_DELEMTER)[1];
        }
        return null;
    }

    interface NetAddress {
        String HTTP = "http";
        String HTTPS = "https";
        String IP_DELEMTER = "://";
        String PORT_DELEMTER = ":";
    }

    /**
     * 网络状态检查
     */
    static class NetworkCheck {
        private static final String PING = "ping ";
        private static final String TTL = "TTL";

        /**
         * 根据系统命令 ping {@code ip} 检查网络状态
         *
         * @param ip ip 地址
         * @return 网络检查结果
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
                try (BufferedReader buffer = new BufferedReader(new InputStreamReader(process.getInputStream(), Charset.forName("GBK")))) {
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
