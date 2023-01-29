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

package org.opengauss.datachecker.check.load;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.service.ShutdownService;
import org.springframework.boot.context.event.ApplicationReadyEvent;

import javax.annotation.Resource;

/**
 * AbstractCheckLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/11/9
 * @since ：11
 */
@Slf4j
public abstract class AbstractCheckLoader implements CheckLoader {
    protected static final int RETRY_TIMES = 10;
    protected static final int HEARTH_RETRY_TIMES = 60;

    @Resource
    private CheckEnvironment checkEnvironment;
    @Resource
    private ShutdownService shutdownService;

    /**
     * Verification environment global information loader
     *
     * @param checkEnvironment checkEnvironment
     */
    @Override
    public abstract void load(CheckEnvironment checkEnvironment);

    /**
     * Handle an application event.
     *
     * @param event the event to respond to
     */
    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        load(checkEnvironment);
    }

    /**
     * shutdown app
     *
     * @param message shutdown message
     */
    public void shutdown(String message) {
        shutdownService.shutdown(message);
    }
}
