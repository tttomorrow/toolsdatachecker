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

package org.opengauss.datachecker.extract;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.ComponentScan;

/**
 * ExtractApplication
 *
 * @author wang chao
 * @date 2022/5/8 19:27
 * @since 11
 **/
@EnableFeignClients(basePackages = {"org.opengauss.datachecker.extract.client"})
@SpringBootApplication
@ComponentScan(value = {"org.opengauss.datachecker.extract", "org.opengauss.datachecker.common"})
public class ExtractApplication {
    public static void main(String[] args) {
        SpringApplication.run(ExtractApplication.class, args);
    }
}
