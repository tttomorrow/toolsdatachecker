package org.opengauss.datachecker.check.client;

import org.opengauss.datachecker.common.entry.check.IncrementCheckConifg;
import org.opengauss.datachecker.common.entry.enums.DML;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.exception.DispatchClientException;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 实现FeignClient 接口调用封装
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
@Service
public class FeignClientService {
    @Autowired
    private ExtractSourceFeignClient extractSourceClient;

    @Autowired
    private ExtractSinkFeignClient extractSinkClient;

    /**
     * 根据端点类型获取指定FeignClient
     *
     * @param endpoint 端点类型
     * @return feignClient
     */
    public ExtractFeignClient getClient(@NonNull Endpoint endpoint) {
        return Endpoint.SOURCE == endpoint ? extractSourceClient : extractSinkClient;
    }

    /**
     * 根据端点类型 获取对应健康状态
     *
     * @param endpoint 端点类型
     * @return 健康状态
     */
    public Result<Void> health(@NonNull Endpoint endpoint) {
        return getClient(endpoint).health();
    }

    /**
     * 加载指定端点的数据库元数据信息
     *
     * @param endpoint 端点类型
     * @return 元数据结果
     */
    public Map<String, TableMetadata> queryMetaDataOfSchema(@NonNull Endpoint endpoint) {
        Result<Map<String, TableMetadata>> result = getClient(endpoint).queryMetaDataOfSchema();
        if (result.isSuccess()) {
            Map<String, TableMetadata> metadata = result.getData();
            return metadata;
        } else {
            //调度源端服务获取数据库元数据信息异常
            throw new DispatchClientException(endpoint, "The scheduling source service gets the database metadata information abnormally," + result.getMessage());
        }
    }


    /**
     * 抽取任务构建
     *
     * @param endpoint  端点类型
     * @param processNo 执行进程编号
     */
    public List<ExtractTask> buildExtractTaskAllTables(@NonNull Endpoint endpoint, String processNo) {
        Result<List<ExtractTask>> result = getClient(endpoint).buildExtractTaskAllTables(processNo);
        if (result.isSuccess()) {
            return result.getData();
        } else {
            //调度抽取服务构建任务异常
            throw new DispatchClientException(endpoint, "The scheduling extraction service construction task is abnormal," + result.getMessage());
        }
    }

    public boolean buildExtractTaskAllTables(@NonNull Endpoint endpoint, String processNo, @NonNull List<ExtractTask> taskList) {
        Result<Void> result = getClient(endpoint).buildExtractTaskAllTables(processNo, taskList);
        if (result.isSuccess()) {
            return result.isSuccess();
        } else {
            //调度抽取服务构建任务异常
            throw new DispatchClientException(endpoint, "The scheduling extraction service construction task is abnormal," + result.getMessage());
        }
    }

    /**
     * 全量抽取业务处理流程
     *
     * @param endpoint  端点类型
     * @param processNo 执行进程序号
     * @return 执行结果
     */
    public boolean execExtractTaskAllTables(@NonNull Endpoint endpoint, String processNo) {
        Result<Void> result = getClient(endpoint).execExtractTaskAllTables(processNo);
        if (result.isSuccess()) {
            return result.isSuccess();
        } else {
            //调度抽取服务执行任务失败
            throw new DispatchClientException(endpoint, "Scheduling extraction service execution task failed," + result.getMessage());
        }
    }

    /**
     * 清理对应端点构建的任务缓存信息 ，任务重置
     *
     * @param endpoint 端点类型
     */
    public void cleanEnvironment(@NonNull Endpoint endpoint, String processNo) {
        getClient(endpoint).cleanEnvironment(processNo);
    }

    public void cleanTask(@NonNull Endpoint endpoint) {
        getClient(endpoint).cleanTask();
    }

    /**
     * 查询指定表对应的Topic信息
     *
     * @param endpoint  端点类型
     * @param tableName 表名称
     * @return Topic信息
     */
    public Topic queryTopicInfo(@NonNull Endpoint endpoint, String tableName) {
        Result<Topic> result = getClient(endpoint).queryTopicInfo(tableName);
        if (result.isSuccess()) {
            return result.getData();
        } else {
            return null;
        }
    }

    public Topic getIncrementTopicInfo(@NonNull Endpoint endpoint, String tableName) {
        Result<Topic> result = getClient(endpoint).getIncrementTopicInfo(tableName);
        if (result.isSuccess()) {
            return result.getData();
        } else {
            return null;
        }
    }

    public List<String> buildRepairDml(Endpoint endpoint, String schema, String tableName, DML dml, Set<String> diffSet) {
        Result<List<String>> result = getClient(endpoint).buildRepairDml(schema, tableName, dml, diffSet);
        if (result.isSuccess()) {
            return result.getData();
        } else {
            return null;
        }
    }

    /**
     * 增量校验日志通知
     *
     * @param endpoint    端点类型
     * @param dataLogList 增量校验日志
     */
    public void notifyIncrementDataLogs(Endpoint endpoint, List<SourceDataLog> dataLogList) {
        getClient(endpoint).notifyIncrementDataLogs(dataLogList);
    }

    public String getDatabaseSchema(Endpoint endpoint) {
        Result<String> result = getClient(endpoint).getDatabaseSchema();
        if (result.isSuccess()) {
            return result.getData();
        } else {
            return null;
        }
    }

    public void configIncrementCheckEnvironment(Endpoint endpoint, IncrementCheckConifg conifg) {
        Result<Void> result = getClient(endpoint).configIncrementCheckEnvironment(conifg);
        if (!result.isSuccess()) {
            throw new CheckingException(result.getMessage());
        }
    }
}
