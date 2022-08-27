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

package org.opengauss.datachecker.extract.task;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.cache.TableExtractStatusCache;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.kafka.KafkaProducerWapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Data extraction thread class
 *
 * @author wang chao
 * @date 2022/5/12 19:17
 * @since 11
 **/
@Slf4j
public class ExtractTaskRunnable extends KafkaProducerWapper implements Runnable {
    private static final String EXTRACT_THREAD_NAME_PREFIX = "EXTRACT_";

    private final Topic topic;
    private final ExtractTask task;
    private final Endpoint endpoint;
    private final String schema;
    private final JdbcTemplate jdbcTemplate;
    private final CheckingFeignClient checkingFeignClient;

    /**
     * Thread Constructor
     *
     * @param task    task information
     * @param topic   Kafka topic information
     * @param support Thread helper class
     */
    public ExtractTaskRunnable(ExtractTask task, Topic topic, ExtractThreadSupport support) {
        super(support.getKafkaProducerConfig());
        this.task = task;
        this.topic = topic;
        schema = support.getExtractProperties().getSchema();
        endpoint = support.getExtractProperties().getEndpoint();
        jdbcTemplate = new JdbcTemplate(support.getDataSourceOne());
        checkingFeignClient = support.getCheckingFeignClient();
    }

    @Override
    public void run() {
        Thread.currentThread().setName(EXTRACT_THREAD_NAME_PREFIX.concat(task.getTaskName()));
        log.info("Data extraction task {} is starting", task.getTaskName());
        TableMetadata tableMetadata = task.getTableMetadata();
        // Construct query SQL according to the metadata information of the table in the current task
        String sql = new SelectSqlBulder(tableMetadata, schema, task.getStart(), task.getOffset()).builder();
        log.debug("selectSql {}", sql);
        // Query data through JDBC SQL
        List<Map<String, String>> dataRowList = queryColumnValues(sql);

        log.info("Data extraction task {} completes basic data query through JDBC", task.getTaskName());
        // Hash the queried data results
        RowDataHashHandler handler = new RowDataHashHandler();
        List<RowDataHash> recordHashList = handler.handlerQueryResult(tableMetadata, dataRowList);

        // Push the data to Kafka according to the fragmentation order
        syncSend(topic, recordHashList);

        String tableName = task.getTableName();
        // If the current task is a sharding task, check the sharding status of the current task before sharding and
        // whether the execution is completed.
        // If the previous sharding task is not completed, wait 100 milliseconds,
        // check again and try until all the previous sharding tasks are completed,
        // and then refresh the current sharding status.
        while (task.isDivisions() && !TableExtractStatusCache.checkCompleted(tableName, task.getDivisionsOrdinal())) {
            log.info("task=[{}] wait divisions of before , send data to kafka completed", task.getTaskName());
            ThreadUtil.sleep(100);
        }
        // When the push is completed, the extraction status of the current task will be updated
        TableExtractStatusCache.update(tableName, task.getDivisionsOrdinal());
        log.info("update extract task={} status completed", task.getTaskName());
        if (!task.isDivisions()) {
            // Notify the verification service that the task data extraction corresponding to
            // the current table has been completed
            checkingFeignClient.refreshTableExtractStatus(tableName, endpoint);
            log.info("refresh table extract status tableName={} status completed", task.getTaskName());
        }
        if (task.isDivisions() && task.getDivisionsOrdinal() == task.getDivisionsTotalNumber()) {
            // The data extraction task of the current table is completed (all subtasks are completed)
            // Notify the verification service that the task data extraction corresponding to
            // the current table has been completed
            checkingFeignClient.refreshTableExtractStatus(tableName, endpoint);
            log.info("refresh table=[{}] extract status completed,task=[{}]", tableName, task.getTaskName());
        }
    }

    private List<Map<String, String>> queryColumnValues(String sql) {
        Map<String, Object> map = new HashMap<>(InitialCapacity.CAPACITY_16);
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(jdbcTemplate);
        return jdbc.query(sql, map, (rs, rowNum) -> {
            ResultSetMetaData metaData = rs.getMetaData();
            ResultSetHandler handler = new ResultSetHandler();
            return handler.putOneResultSetToMap(rs, metaData);
        });
    }
}
