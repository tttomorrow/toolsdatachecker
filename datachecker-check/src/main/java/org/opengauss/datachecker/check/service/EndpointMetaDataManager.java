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
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * EndpointMetaDataManager
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/24
 * @since ：11
 */
@Slf4j
@Service
public class EndpointMetaDataManager {
    private static final List<String> CHECK_TABLE_LIST = new ArrayList<>();

    @Autowired
    private EndpointStatusManager endpointStatusManager;

    @Autowired
    private FeignClientService feignClientService;

    /**
     * Reload metadata information
     */
    public void load() {
        CHECK_TABLE_LIST.clear();
        final Map<String, TableMetadata> metadataMap = feignClientService.queryMetaDataOfSchema(Endpoint.SOURCE);
        feignClientService.queryMetaDataOfSchema(Endpoint.SINK);
        if (!metadataMap.isEmpty()) {
            CHECK_TABLE_LIST.addAll(
                metadataMap.values().stream().sorted(Comparator.comparing(TableMetadata::getTableRows))
                           .map(TableMetadata::getTableName).collect(Collectors.toUnmodifiableList()));
        }
        log.info("Load endpoint metadata information");
    }

    /**
     * View the health status of all endpoints
     *
     * @return health status
     */
    public boolean isEndpointHealth() {
        return endpointStatusManager.isEndpointHealth();
    }

    /**
     * Return to the verification black and white list
     *
     * @return black and white list
     */
    public List<String> getCheckTableList() {
        return CHECK_TABLE_LIST;
    }
}
