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

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.util.TaskUtil;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.kafka.KafkaProducerWapper;
import org.opengauss.datachecker.extract.task.sql.SelectSqlBuilder;
import org.opengauss.datachecker.extract.util.MetaDataUtil;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Data extraction thread class
 *
 * @author wang chao
 * @date 2022/5/12 19:17
 * @since 11
 **/
@Slf4j
public class ExtractTaskRunnable extends KafkaProducerWapper implements Runnable {
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
        super(support.getKafkaTemplate());
        this.task = task;
        this.topic = topic;
        databaseType = support.getExtractProperties().getDatabaseType();
        schema = support.getExtractProperties().getSchema();
        endpoint = support.getExtractProperties().getEndpoint();
        jdbcTemplate = new JdbcTemplate(support.getDataSourceOne());
        checkingFeignClient = support.getCheckingFeignClient();
        resultSetFactory = new ResultSetHandlerFactory();
    }

    @SneakyThrows
    @Override
    public void run() {
        Thread.currentThread().setName(task.getTaskName() + "_" + Thread.currentThread().getId());
        TableMetadata tableMetadata = task.getTableMetadata();
        // Construct query SQL according to the metadata information of the table in the current task
        List<String> querySqlList = buildQuerySqlList(tableMetadata);
        executeTask(querySqlList, tableMetadata);
        checkingFeignClient.refreshTableExtractStatus(task.getTableName(), endpoint, endpoint.getCode());
    }

    private List<String> buildQuerySqlList(TableMetadata tableMetadata) {
        List<String> queryList = new ArrayList<>();
        final int[][] taskOffset = TaskUtil.calcAutoTaskOffset(tableMetadata.getTableRows());
        final int taskCount = taskOffset.length;
        final SelectSqlBuilder sqlBuilder = new SelectSqlBuilder(tableMetadata, schema);
        IntStream.range(0, taskCount).forEach(idx -> {
            final String querySql = sqlBuilder.dataBaseType(databaseType).isDivisions(task.isDivisions())
                                              .offset(taskOffset[idx][0], taskOffset[idx][1]).builder();
            log.info("query table[{}] sql: [{}]", tableMetadata.getTableName(), querySql);
            queryList.add(querySql);
        });
        return queryList;
    }

    private void executeTask(List<String> querySqlList, TableMetadata tableMetadata) throws InterruptedException {
        final String tableName = tableMetadata.getTableName();
        final LocalDateTime start = LocalDateTime.now();

        ResultSetHashHandler resultSetHashHandler = new ResultSetHashHandler();
        ResultSetHandler resultSetHandler = resultSetFactory.createHandler(databaseType);
        List<String> columns = MetaDataUtil.getTableColumns(tableMetadata);
        List<String> primary = MetaDataUtil.getTablePrimaryColumns(tableMetadata);
        if (querySqlList.size() > 1) {
            try {
                CountDownLatch countDownLatch = new CountDownLatch(querySqlList.size());
                querySqlList.parallelStream().forEach(sql -> {
                    try (final Stream<RowDataHash> resultStream = jdbcTemplate.queryForStream(sql,
                        (RowMapper<RowDataHash>) (rs, rowNum) -> resultSetHashHandler
                            .handler(primary, columns, resultSetHandler.putOneResultSetToMap(rs)))) {
                        // Push the data to Kafka according to the fragmentation order
                        syncSend(topic, resultStream.collect(Collectors.toList()));
                    } catch (DataAccessException exception) {
                        log.error("jdbc query stream [{}] error : {}", sql, exception.getMessage());
                    } finally {
                        countDownLatch.countDown();
                        if (countDownLatch.getCount() > 0) {
                            log.info("extract table [{}] remaining [{}] tasks", tableName, countDownLatch.getCount());
                        }
                    }
                });
                countDownLatch.await();
            } catch (InterruptedException ex) {
                log.error("jdbc query stream count latch error [{}] : {}", tableName, ex.getMessage());
            }
        } else if (querySqlList.size() == 1) {
            String queryAllTables = querySqlList.get(0);
            try (Stream<RowDataHash> resultStream = jdbcTemplate.queryForStream(queryAllTables,
                (RowMapper<RowDataHash>) (rs, rowNum) -> resultSetHashHandler
                    .handler(primary, columns, resultSetHandler.putOneResultSetToMap(rs)))) {
                // Push the data to Kafka according to the fragmentation order
                syncSend(topic, resultStream.collect(Collectors.toList()));
            } catch (DataAccessException exception) {
                log.error("jdbc query stream [{}] error : {}", queryAllTables, exception.getMessage());
            }
        }
        final LocalDateTime end = LocalDateTime.now();
        log.info("extract table[{}] cost [{}] millis", tableName, Duration.between(start, end).toMillis());
    }
}
