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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.exception.CheckMetaDataException;
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
@RequiredArgsConstructor
public class EndpointMetaDataManager {
    private static final List<String> CHECK_TABLE_LIST = new ArrayList<>();

    private final EndpointStatusManager endpointStatusManager;
    private final FeignClientService feignClientService;

    /**
     * Reload metadata information
     */
    public void load() {
        CHECK_TABLE_LIST.clear();
        final Map<String, TableMetadata> sourceMetadataMap = feignClientService.queryMetaDataOfSchema(Endpoint.SOURCE);
        final Map<String, TableMetadata> sinkMetadataMap = feignClientService.queryMetaDataOfSchema(Endpoint.SINK);
        if (MapUtils.isNotEmpty(sourceMetadataMap) && MapUtils.isNotEmpty(sinkMetadataMap)) {
            final List<String> sourceTables = getEndpointTableNamesSortByTableRows(sourceMetadataMap);
            final List<String> sinkTables = getEndpointTableNamesSortByTableRows(sinkMetadataMap);
            final List<String> checkTables = compareAndFilterEndpointTables(sourceTables, sinkTables);
            CHECK_TABLE_LIST.addAll(checkTables);
            log.info("Load endpoint metadata information");
        } else {
            log.error("The metadata information is empty, and the verification is terminated abnormally,"
                + "sourceMetadata={},sinkMetadata={}", sourceMetadataMap.size(), sinkMetadataMap.size());
            throw new CheckMetaDataException(
                "The metadata information is empty, and the verification is terminated abnormally");
        }
    }

    private List<String> compareAndFilterEndpointTables(List<String> sourceTables, List<String> sinkTables) {
        return sourceTables.stream().filter(table -> sinkTables.contains(table)).collect(Collectors.toList());
    }

    private List<String> getEndpointTableNamesSortByTableRows(Map<String, TableMetadata> metadataMap) {
        return metadataMap.values().stream().sorted(Comparator.comparing(TableMetadata::getTableRows))
                          .map(TableMetadata::getTableName).collect(Collectors.toUnmodifiableList());
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
