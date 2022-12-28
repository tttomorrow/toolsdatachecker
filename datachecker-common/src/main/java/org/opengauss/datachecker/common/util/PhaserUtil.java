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

package org.opengauss.datachecker.common.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;

/**
 * PhaserUtil
 *
 * @author ：wangchao
 * @date ：Created in 2022/12/22
 * @since ：11
 */
@Slf4j
public class PhaserUtil {

    public static void submit(ThreadPoolTaskExecutor executorService, List<Runnable> tasks, Runnable complete) {
        final List<Future<?>> futureList = new ArrayList<>();
        for (Runnable runnable : tasks) {
            futureList.add(executorService.submit(() -> {
                runnable.run();
            }));
        }
        futureList.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("future  error: {}", e.getMessage());
            }
        });
        complete.run();
    }
}
