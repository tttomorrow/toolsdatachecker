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

package org.opengauss.datachecker.check.client;

import org.opengauss.datachecker.common.entry.check.IncrementCheckConfig;
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
 * Implement feign client interface call encapsulation
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
     * Get the specified feign client according to the endpoint type
     *
     * @param endpoint endpoint type
     * @return feignClient
     */
    public ExtractFeignClient getClient(@NonNull Endpoint endpoint) {
        return Endpoint.SOURCE == endpoint ? extractSourceClient : extractSinkClient;
    }

    /**
     * Service health check
     *
     * @param endpoint endpoint type
     * @return Return the corresponding result of the interface
     */
    public Result<Void> health(@NonNull Endpoint endpoint) {
        return getClient(endpoint).health();
    }

    /**
     * Endpoint loading metadata information
     *
     * @param endpoint endpoint type
     * @return Return metadata
     */
    public Map<String, TableMetadata> queryMetaDataOfSchema(@NonNull Endpoint endpoint) {
        Result<Map<String, TableMetadata>> result = getClient(endpoint).queryMetaDataOfSchema();
        if (result.isSuccess()) {
            Map<String, TableMetadata> metadata = result.getData();
            return metadata;
        } else {
            // Exception in scheduling source side service to obtain database metadata information
            throw new DispatchClientException(endpoint,
                "The scheduling source service gets the database metadata information abnormally," + result
                    .getMessage());
        }
    }

    /**
     * Extraction task construction
     *
     * @param endpoint  endpoint type
     * @param processNo Execution process number
     * @return Return to build task collection
     */
    public List<ExtractTask> buildExtractTaskAllTables(@NonNull Endpoint endpoint, String processNo) {
        Result<List<ExtractTask>> result = getClient(endpoint).buildExtractTaskAllTables(processNo);
        if (result.isSuccess()) {
            return result.getData();
        } else {
            // Scheduling extraction service construction task exception
            throw new DispatchClientException(endpoint,
                "The scheduling extraction service construction task is abnormal," + result.getMessage());
        }
    }

    /**
     * Destination extraction task configuration
     *
     * @param endpoint  endpoint type
     * @param processNo Execution process number
     * @param taskList  Source side task list
     * @return Request results
     */
    public boolean buildExtractTaskAllTables(@NonNull Endpoint endpoint, String processNo,
        @NonNull List<ExtractTask> taskList) {
        Result<Void> result = getClient(endpoint).buildExtractTaskAllTables(processNo, taskList);
        if (result.isSuccess()) {
            return result.isSuccess();
        } else {
            // Scheduling extraction service construction task exception
            throw new DispatchClientException(endpoint,
                "The scheduling extraction service construction task is abnormal," + result.getMessage());
        }
    }

    /**
     * Full extraction business processing flow
     *
     * @param endpoint  endpoint type
     * @param processNo Execution process sequence number
     * @return Request results
     */
    public boolean execExtractTaskAllTables(@NonNull Endpoint endpoint, String processNo) {
        Result<Void> result = getClient(endpoint).execExtractTaskAllTables(processNo);
        if (result.isSuccess()) {
            return result.isSuccess();
        } else {
            // Scheduling extraction service execution task failed
            throw new DispatchClientException(endpoint,
                "Scheduling extraction service execution task failed," + result.getMessage());
        }
    }

    /**
     * Clean up the opposite environment
     *
     * @param endpoint  endpoint type
     * @param processNo Execution process sequence number
     */
    public void cleanEnvironment(@NonNull Endpoint endpoint, String processNo) {
        getClient(endpoint).cleanEnvironment(processNo);
    }

    /**
     * Clear the extraction end task cache
     *
     * @param endpoint endpoint type
     */
    public void cleanTask(@NonNull Endpoint endpoint) {
        getClient(endpoint).cleanTask();
    }

    /**
     * Query the topic information corresponding to the specified table
     *
     * @param endpoint  endpoint type
     * @param tableName tableName
     * @return Topic information
     */
    public Topic queryTopicInfo(@NonNull Endpoint endpoint, String tableName) {
        Result<Topic> result = getClient(endpoint).queryTopicInfo(tableName);
        if (result.isSuccess()) {
            return result.getData();
        } else {
            return null;
        }
    }

    /**
     * Get incremental topic information
     *
     * @param endpoint  endpoint type
     * @param tableName table Name
     * @return Return the topic information corresponding to the table
     */
    public Topic getIncrementTopicInfo(@NonNull Endpoint endpoint, String tableName) {
        Result<Topic> result = getClient(endpoint).getIncrementTopicInfo(tableName);
        if (result.isSuccess()) {
            return result.getData();
        } else {
            return null;
        }
    }

    /**
     * Build repair statements based on parameters
     *
     * @param endpoint  endpoint type
     * @param schema    The corresponding schema of the end DB to be repaired
     * @param tableName table Name
     * @param dml       Repair type {@link DML}
     * @param diffSet   Differential primary key set
     * @return Return to repair statement collection
     */
    public List<String> buildRepairDml(Endpoint endpoint, String schema, String tableName, DML dml,
        Set<String> diffSet) {
        Result<List<String>> result = getClient(endpoint).buildRepairDml(schema, tableName, dml, diffSet);
        if (result.isSuccess()) {
            return result.getData();
        } else {
            return null;
        }
    }

    /**
     * Issue incremental log data
     *
     * @param endpoint    endpoint type
     * @param dataLogList incremental log data
     */
    public void notifyIncrementDataLogs(Endpoint endpoint, List<SourceDataLog> dataLogList) {
        getClient(endpoint).notifyIncrementDataLogs(dataLogList);
    }

    /**
     * Query the schema information of the extraction end database
     *
     * @param endpoint endpoint type
     * @return schema
     */
    public String getDatabaseSchema(Endpoint endpoint) {
        Result<String> result = getClient(endpoint).getDatabaseSchema();
        if (result.isSuccess()) {
            return result.getData();
        } else {
            return null;
        }
    }

    /**
     * Configure the configuration information related to debezium in the incremental verification scenario
     *
     * @param endpoint endpoint type
     * @param conifg   Debezium related configurations
     */
    public void configIncrementCheckEnvironment(Endpoint endpoint, IncrementCheckConfig conifg) {
        Result<Void> result = getClient(endpoint).configIncrementCheckEnvironment(conifg);
        if (!result.isSuccess()) {
            throw new CheckingException(result.getMessage());
        }
    }
}
