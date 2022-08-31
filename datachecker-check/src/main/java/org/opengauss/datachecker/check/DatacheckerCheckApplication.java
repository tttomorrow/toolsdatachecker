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

package org.opengauss.datachecker.check;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.check.service.EndpointManagerService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * DatacheckerCheckApplication
 *
 * @author wang chao
 * @date 2022/5/8 19:27
 * @since 11
 **/
@Slf4j
@EnableAsync
@EnableFeignClients(basePackages = {"org.opengauss.datachecker.check.client"})
@SpringBootApplication
public class DatacheckerCheckApplication {
    private static ConfigurableApplicationContext context;

    public static void main(String[] args) {
        context = SpringApplication.run(DatacheckerCheckApplication.class, args);
        final EndpointManagerService managerService = context.getBean(EndpointManagerService.class);
        managerService.start();
        if (!managerService.isEndpointHealth()) {
            log.error("The verification service failed to start due to the abnormal state of the endpoint service");
            managerService.shutdown();
            context.close();
        }
    }
}
