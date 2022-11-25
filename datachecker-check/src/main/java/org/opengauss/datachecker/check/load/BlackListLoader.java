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
import org.opengauss.datachecker.check.service.CheckBlackWhiteService;
import org.opengauss.datachecker.check.service.EndpointMetaDataManager;
import org.opengauss.datachecker.common.entry.enums.CheckBlackWhiteMode;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Objects;

/**
 * MetaDataLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/11/9
 * @since ：11
 */
@Slf4j
@Order(101)
@Service
public class BlackListLoader extends AbstractCheckLoader {
    @Value("${data.check.black-list}")
    private String[] backList;
    @Value("${data.check.black-white-mode}")
    private CheckBlackWhiteMode blackWhiteMode;
    @Resource
    private CheckBlackWhiteService checkBlackWhiteService;

    @Override
    public void load(CheckEnvironment checkEnvironment) {
        try {
            if (Objects.equals(blackWhiteMode, CheckBlackWhiteMode.BLACK)) {
                if (backList.length != 0) {
                    checkBlackWhiteService.addBlackList(Arrays.asList(backList));
                }
                log.info("check service load back list success.{}", backList);
            }
        } catch (CheckingException ex) {
            log.error("check service load back list exception :", ex);
        }
    }
}
