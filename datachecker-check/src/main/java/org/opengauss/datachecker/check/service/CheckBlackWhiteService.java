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

package org.opengauss.datachecker.check.service;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.config.DataCheckProperties;
import org.opengauss.datachecker.common.entry.enums.CheckBlackWhiteMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/22
 * @since ：11
 */
@Slf4j
@Service
public class CheckBlackWhiteService {
    private static final Set<String> WHITE = new ConcurrentSkipListSet<>();
    private static final Set<String> BLACK = new ConcurrentSkipListSet<>();

    @Autowired
    private FeignClientService feignClientService;

    @Autowired
    private DataCheckProperties dataCheckProperties;

    @Autowired
    private EndpointMetaDataManager endpointMetaDataManager;

    /**
     * Add white list this function clears the historical white list and resets the white list to the current list
     *
     * @param whiteList whiteList
     */
    public void addWhiteList(List<String> whiteList) {
        WHITE.clear();
        WHITE.addAll(whiteList);
        refreshBlackWhiteList();
        log.info("add whitelist list [{}]", whiteList);
    }

    /**
     * Update white list this function adds the current list to the white list on the basis of the current white list
     *
     * @param whiteList whiteList
     */
    public void updateWhiteList(List<String> whiteList) {
        WHITE.addAll(whiteList);
        refreshBlackWhiteList();
        log.info("update whitelist list [{}]", whiteList);
    }

    /**
     * Remove white list this function removes the current list from the current white list
     *
     * @param whiteList whiteList
     */
    public void deleteWhiteList(List<String> whiteList) {
        WHITE.removeAll(whiteList);
        refreshBlackWhiteList();
        log.info("delete whitelist list [{}]", whiteList);
    }

    /**
     * Query white list
     *
     * @return whiteList
     */
    public List<String> queryWhiteList() {
        return new ArrayList<>(WHITE);
    }

    public void addBlackList(List<String> blackList) {
        BLACK.clear();
        BLACK.addAll(blackList);
        refreshBlackWhiteList();
        log.info("add blackList list [{}]", blackList);
    }

    public void updateBlackList(List<String> blackList) {
        BLACK.addAll(blackList);
        refreshBlackWhiteList();
        log.info("update blackList list [{}]", blackList);
    }

    public void deleteBlackList(List<String> blackList) {
        BLACK.removeAll(blackList);
        refreshBlackWhiteList();
        log.info("delete blackList list [{}]", blackList);
    }

    public List<String> queryBlackList() {
        return new ArrayList<>(BLACK);
    }

    private void refreshBlackWhiteList() {
        final CheckBlackWhiteMode blackWhiteMode = dataCheckProperties.getBlackWhiteMode();
        if (blackWhiteMode == CheckBlackWhiteMode.WHITE) {
            // White list mode
            feignClientService.getClient(Endpoint.SOURCE).refreshBlackWhiteList(blackWhiteMode, new ArrayList<>(WHITE));
            feignClientService.getClient(Endpoint.SINK).refreshBlackWhiteList(blackWhiteMode, new ArrayList<>(WHITE));
        } else if (blackWhiteMode == CheckBlackWhiteMode.BLACK) {
            // Blacklist mode
            feignClientService.getClient(Endpoint.SOURCE).refreshBlackWhiteList(blackWhiteMode, new ArrayList<>(BLACK));
            feignClientService.getClient(Endpoint.SINK).refreshBlackWhiteList(blackWhiteMode, new ArrayList<>(BLACK));
        }
        endpointMetaDataManager.load();
    }

}
