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

package org.opengauss.datachecker.extract.load;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ConfigurableApplicationContext;

import javax.annotation.Resource;

/**
 * AbstractCheckLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/11/9
 * @since ：11
 */
@Slf4j
public abstract class AbstractExtractLoader implements ExtractLoader {
    private static ConfigurableApplicationContext applicationContext;
    @Resource
    private ExtractEnvironment extractEnvironment;

    /**
     * Verification environment global information loader
     */
    @Override
    public abstract void load(ExtractEnvironment extractEnvironment);

    /**
     * Handle an application event.
     *
     * @param event the event to respond to
     */
    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        applicationContext = event.getApplicationContext();
        load(extractEnvironment);
    }

    /**
     * shutdown app
     *
     * @param message shutdown message
     */
    public void shutdown(String message) {
        log.error("The check server will be shutdown , {}", message);
        log.error("check server exited .");
        System.exit(SpringApplication.exit(applicationContext));
    }
}
