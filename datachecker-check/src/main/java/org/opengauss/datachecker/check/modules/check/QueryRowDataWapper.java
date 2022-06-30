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
     * 拉取指定端点{@code endpoint}的表{@code tableName}的 kafka分区{@code partitions}数据
     *
     * @param endpoint   端点类型
     * @param partitions kafka分区号
     * @return 指定表 kafka分区数据
     */
    public List<RowDataHash> queryRowData(Endpoint endpoint, String tableName, int partitions) {
        List<RowDataHash> data = new ArrayList<>();
        Result<List<RowDataHash>> result = feignClient.getClient(endpoint).queryTopicData(tableName, partitions);
        if (!result.isSuccess()) {
            throw new DispatchClientException(endpoint, "query topic data of tableName " + tableName +
                    " partitions=" + partitions + " error, " + result.getMessage());
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
            throw new DispatchClientException(endpoint, "query topic data of tableName " + tableName +
                    " error, " + result.getMessage());
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
            throw new DispatchClientException(endpoint, "query topic data of tableName " + dataLog.getTableName() +
                    " error, " + result.getMessage());
        }
        return result.getData();
    }
}
