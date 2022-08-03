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

package org.opengauss.datachecker.check.modules.check;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.exception.DispatchClientException;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/18
 * @since ：11
 */
@Slf4j
public class QueryRowDataWapper {

    private final FeignClientService feignClient;

    public QueryRowDataWapper(FeignClientService feignClient) {
        this.feignClient = feignClient;
    }

    /**
     * Pull the Kafka partition {@code partitions} data
     * of the table {@code tableName} of the specified endpoint {@code endpoint}
     *
     * @param endpoint   endpoint
     * @param partitions kafka partitions
     * @return Specify table Kafka partition data
     */
    public List<RowDataHash> queryRowData(Endpoint endpoint, String tableName, int partitions) {
        List<RowDataHash> data = new ArrayList<>();
        Result<List<RowDataHash>> result = feignClient.getClient(endpoint).queryTopicData(tableName, partitions);
        if (!result.isSuccess()) {
            throw new DispatchClientException(endpoint,
                "query topic data of tableName " + tableName + " partitions=" + partitions + " error, " + result
                    .getMessage());
        }
        while (result.isSuccess() && !CollectionUtils.isEmpty(result.getData())) {
            data.addAll(result.getData());
            result = feignClient.getClient(endpoint).queryTopicData(tableName, partitions);
        }
        return data;
    }

    public List<RowDataHash> queryIncrementRowData(Endpoint endpoint, String tableName) {
        List<RowDataHash> data = new ArrayList<>();
        Result<List<RowDataHash>> result = feignClient.getClient(endpoint).queryIncrementTopicData(tableName);
        if (!result.isSuccess()) {
            throw new DispatchClientException(endpoint,
                "query topic data of tableName " + tableName + " error, " + result.getMessage());
        }
        while (result.isSuccess() && !CollectionUtils.isEmpty(result.getData())) {
            data.addAll(result.getData());
            result = feignClient.getClient(endpoint).queryIncrementTopicData(tableName);
        }
        return data;
    }

    public List<RowDataHash> queryRowData(Endpoint endpoint, SourceDataLog dataLog) {
        Result<List<RowDataHash>> result = feignClient.getClient(endpoint).querySecondaryCheckRowData(dataLog);
        if (!result.isSuccess()) {
            throw new DispatchClientException(endpoint,
                "query topic data of tableName " + dataLog.getTableName() + " error, " + result.getMessage());
        }
        return result.getData();
    }
}
