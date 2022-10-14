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
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.exception.ExtractException;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.cache.TableExtractStatusCache;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.kafka.KafkaProducerWapper;
import org.opengauss.datachecker.extract.task.sql.SelectSqlBuilder;
import org.opengauss.datachecker.extract.util.MetaDataUtil;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.HashMap;
import java.util.List;

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
    private static final String EXTRACT_STATUS_THREAD_NAME_PREFIX = "EXTRACT_STATUS_";

    private final Topic topic;
    private final ExtractTask task;
    private final Endpoint endpoint;
    private final DataBaseType databaseType;
    private final String schema;
    private final JdbcTemplate jdbcTemplate;
    private final CheckingFeignClient checkingFeignClient;
    private final ResultSetHandlerFactory resultSetFactory;

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
        databaseType = support.getExtractProperties().getDatabaseType();
        schema = support.getExtractProperties().getSchema();
        endpoint = support.getExtractProperties().getEndpoint();
        jdbcTemplate = new JdbcTemplate(support.getDataSourceOne());
        checkingFeignClient = support.getCheckingFeignClient();
        resultSetFactory = new ResultSetHandlerFactory();
    }

    /**
     * Core logic of data extraction task execution
     */
    public void executeTask() {
        TableMetadata tableMetadata = task.getTableMetadata();
        // Construct query SQL according to the metadata information of the table in the current task
        final SelectSqlBuilder sqlBuilder = new SelectSqlBuilder(tableMetadata, schema);
        String sql = sqlBuilder.dataBaseType(databaseType).isDivisions(task.isDivisions())
                               .offset(task.getStart(), task.getOffset()).builder();
        log.debug("selectSql {}", sql);
        // Query data through JDBC SQL and Hash the queried data results ,
        // then package data into RowDataHash type Objects
        log.info("Data extraction task {} start, data query through jdbc", task.getTaskName());
        List<RowDataHash> recordHashList = queryAndConvertColumnValues(sql, tableMetadata);
        log.info("Data extraction task {} completes, data query through jdbc", task.getTaskName());

        // Push the data to Kafka according to the fragmentation order
        syncSend(topic, recordHashList);

        ThreadUtil.newSingleThreadExecutor().submit(() -> {
            Thread.currentThread().setName(EXTRACT_STATUS_THREAD_NAME_PREFIX.concat(task.getTaskName()));
            String tableName = task.getTableName();
            // When the push is completed, the extraction status of the current task will be updated
            TableExtractStatusCache.update(tableName, task.getDivisionsOrdinal());
            if (!task.isDivisions()) {
                // Notify the verification service that the task data extraction corresponding to
                // the current table has been completed
                checkingFeignClient.refreshTableExtractStatus(tableName, endpoint, endpoint.getCode());
                log.info("refresh table extract status tableName={} status completed", task.getTaskName());
            }
            if (task.isDivisions() && task.getDivisionsOrdinal() == task.getDivisionsTotalNumber()) {
                // The data extraction task of the current table is completed (all subtasks are completed)
                // Notify the verification service that the task data extraction corresponding to
                // the current table has been completed
                while (!TableExtractStatusCache.checkCompleted(tableName, task.getDivisionsOrdinal())) {
                    ThreadUtil.sleep(1000);
                    if (TableExtractStatusCache.hasErrorOccurred(tableName)) {
                        break;
                    }
                }
                if (TableExtractStatusCache.hasErrorOccurred(tableName)) {
                    checkingFeignClient.refreshTableExtractStatus(tableName, endpoint, -1);
                    log.error("refresh table=[{}] extract status error,task=[{}]", tableName, task.getTaskName());
                } else {
                    checkingFeignClient.refreshTableExtractStatus(tableName, endpoint, endpoint.getCode());
                    log.info("refresh table=[{}] extract status completed,task=[{}]", tableName, task.getTaskName());
                }
            }
        });
    }

    private List<RowDataHash> queryAndConvertColumnValues(String sql, TableMetadata tableMetadata) {
        List<String> columns = MetaDataUtil.getTableColumns(tableMetadata);
        List<String> primary = MetaDataUtil.getTablePrimaryColumns(tableMetadata);
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(jdbcTemplate);
        ResultSetHashHandler resultSetHashHandler = new ResultSetHashHandler();
        ResultSetHandler resultSetHandler = resultSetFactory.createHandler(databaseType);
        return jdbc.query(sql, new HashMap<>(InitialCapacity.EMPTY),
            (rs, rowNum) -> resultSetHashHandler.handler(primary, columns, resultSetHandler.putOneResultSetToMap(rs)));
    }

    @Override
    public void run() {
        Thread.currentThread().setName(EXTRACT_THREAD_NAME_PREFIX.concat(task.getTaskName()));
        final String tableName = task.getTableName();
        log.info("Data extraction task {} is starting", tableName);
        try {
            if (TableExtractStatusCache.hasErrorOccurred(tableName)) {
                log.error("table:[{}] has some error,current task=[{}] is canceled", tableName, task.getTaskName());
                return;
            }
            executeTask();
        } catch (RuntimeException exp) {
            log.error("Data extraction task {} has some exception,{}", task.getTaskName(), exp.getMessage());
            TableExtractStatusCache.addErrorList(tableName);
        }
    }
}
