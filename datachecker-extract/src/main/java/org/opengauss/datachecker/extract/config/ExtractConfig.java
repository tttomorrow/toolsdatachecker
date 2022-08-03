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

package org.opengauss.datachecker.extract.config;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/23
 * @since ：11
 */
@Slf4j
@Component
public class ExtractConfig {
    @Autowired
    private ExtractProperties extractProperties;

    /**
     * Start loading check config properties
     */
    @PostConstruct
    public void initLoad() {
        log.info("check config properties [{}]", JsonObjectUtil.format(extractProperties));
    }
}
