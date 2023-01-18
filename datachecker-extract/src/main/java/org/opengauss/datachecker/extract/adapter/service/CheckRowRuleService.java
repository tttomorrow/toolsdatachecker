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

package org.opengauss.datachecker.extract.adapter.service;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.common.Rule;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.annotation.Resource;

/**
 * CheckRowRuleService
 *
 * @author ：wangchao
 * @date ：Created in 2022/12/2
 * @since ：11
 */
@Slf4j
public abstract class CheckRowRuleService implements CheckRowRule {
    @Resource
    private JdbcTemplate jdbcTemplate;

    @Override
    public boolean checkRule(String schema, Rule rule) {
        boolean result = false;
        try {
            String checkStatement = String.format(CHECK_RULE_STATEMENT, convert(schema), convert(rule.getName()),
                convertCondition(rule.getText()));
            jdbcTemplate.execute(checkStatement);
            result = true;
        } catch (DataAccessException ex) {
            log.error("rules of row is invalid!,schema={},rule={}", schema, rule);
        }
        return result;
    }

    /**
     * convert text
     *
     * @param text text
     * @return text
     */
    protected abstract String convert(String text);

    /**
     * Row level rule condition semantics support ( > < >= <= = !=) Six types of conditional filtering
     * and compound conditional statements;
     * Composite conditional statements must be spliced with and, for example, a>1 and b>2;
     *
     * @param text text
     * @return condition
     */
    protected abstract String convertCondition(String text);
}
