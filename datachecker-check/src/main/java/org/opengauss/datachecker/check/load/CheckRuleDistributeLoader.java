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
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.config.RuleConfig;
import org.opengauss.datachecker.check.modules.rule.RuleParser;
import org.opengauss.datachecker.common.entry.common.Rule;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.enums.RuleType;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * CheckRuleLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/10/31
 * @since ：11
 */
@Slf4j
@Order(98)
@Service
public class CheckRuleDistributeLoader extends AbstractCheckLoader {
    @Resource
    private FeignClientService feignClient;
    @Resource
    private RuleConfig config;

    /**
     * Initialize the verification result environment
     */
    @Override
    public void load(CheckEnvironment checkEnvironment) {
        try {
            log.info("check service distribute rules.");
            RuleParser ruleParser = new RuleParser();
            CheckMode checkMode = checkEnvironment.getCheckMode();
            if (Objects.equals(CheckMode.INCREMENT, checkMode)) {
                config.getTable().clear();
                config.getRow().clear();
            }
            final Map<RuleType, List<Rule>> rules = ruleParser.parser(config);
            feignClient.distributeRules(Endpoint.SOURCE, checkMode, rules);
            feignClient.distributeRules(Endpoint.SINK, checkMode, rules);
            checkEnvironment.addRules(rules);
            log.info("check service distribute rules success.");
        } catch (Exception ignore) {
            log.error("distribute rules error: ", ignore);
            throw new CheckingException("distribute rules error");
        }
    }
}
