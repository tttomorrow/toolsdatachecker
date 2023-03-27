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
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;
import org.opengauss.datachecker.common.exception.ExtractException;
import org.opengauss.datachecker.common.util.TaskUtil;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.kafka.KafkaProducerWapper;
import org.opengauss.datachecker.extract.task.sql.SelectSqlBuilder;
import org.opengauss.datachecker.extract.util.MetaDataUtil;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
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
    private final ConnectionManager connectionManager;
    private final CheckingFeignClient checkingFeignClient;
    private static final String OPEN_GAUSS_PARALLEL_QUERY = "set query_dop to %s;";

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
        connectionManager = support.getConnectionManager();
        jdbcTemplate = new JdbcTemplate(support.getDataSourceOne());
        checkingFeignClient = support.getCheckingFeignClient();
    }

    @SneakyThrows
    @Override
    public void run() {
        Thread.currentThread().setName(task.getTaskName() + "_" + Thread.currentThread().getId());
        TableMetadata tableMetadata = task.getTableMetadata();
        QueryTableRowContext context = new QueryTableRowContext(tableMetadata, databaseType);
        // Construct query SQL according to the metadata information of the table in the current task
        final int[][] taskOffset = TaskUtil.calcAutoTaskOffset(tableMetadata.getTableRows());
        try {
            if (taskOffset.length >= 2) {
                executeMultiTaskOffset(taskOffset, tableMetadata, context);
            } else {
                executeTask(taskOffset, tableMetadata, context);
            }
            checkingFeignClient.refreshTableExtractStatus(task.getTableName(), endpoint, endpoint.getCode());
        } catch (ExtractDataAccessException ex) {
            checkingFeignClient.refreshTableExtractStatus(task.getTableName(), endpoint, -1);
        }
    }

    private void enableDatabaseParallelQuery(int queryDop) {
        if (Objects.equals(DataBaseType.OG, databaseType)) {
            jdbcTemplate.execute(String.format(OPEN_GAUSS_PARALLEL_QUERY, queryDop));
        }
    }

    private void executeMultiTaskOffset(int[][] taskOffset, TableMetadata tableMetadata, QueryTableRowContext context) {
        if (taskOffset == null || taskOffset.length < 2) {
            return;
        }
        final LocalDateTime start = LocalDateTime.now();
        List<String> querySqlList = new ArrayList<>();
        builderQuerySqlByTaskOffset(taskOffset, tableMetadata, querySqlList);
        String tableName = tableMetadata.getTableName();
        try {
            CountDownLatch countDownLatch = new CountDownLatch(taskOffset.length);
            int dop = Math.min(taskOffset.length, connectionManager.getParallelQueryDop());
            enableDatabaseParallelQuery(dop);
            ForkJoinPool customThreadPool = new ForkJoinPool(dop);
            customThreadPool.submit(() -> {
                querySqlList.parallelStream().map(sql -> {
                    takeConnection();
                    try (final Stream<RowDataHash> resultStream = jdbcTemplate
                        .queryForStream(sql, (RowMapper<RowDataHash>) (rs, rowNum) -> context.resultSetHandler(rs))) {
                        // Push the data to Kafka according to the fragmentation order
                        syncSend(topic, resultStream.collect(Collectors.toList()));
                    } catch (DataAccessException exception) {
                        log.error("jdbc query stream [{}] error : {}", sql, exception.getMessage());
                        throw new ExtractDataAccessException();
                    } finally {
                        countDownLatch.countDown();
                        if (countDownLatch.getCount() > 0) {
                            log.info("extract table [{}] remaining [{}] tasks", tableName, countDownLatch.getCount());
                        }
                        releaseConnection();
                    }
                    return Duration.between(start, LocalDateTime.now()).toMillis();
                }).collect(Collectors.toList());
            }).get();
            countDownLatch.await();

            // Fix inaccurate statistics of the total number of table row records
            final SelectSqlBuilder sqlBuilder = new SelectSqlBuilder(tableMetadata, schema);

            long fixOffset = taskOffset[taskOffset.length - 1][1];
            long fixStart = taskOffset[taskOffset.length - 1][0] + fixOffset;
            AtomicLong queryRowSize = new AtomicLong(fixOffset);
            while (queryRowSize.get() > 0) {
                final String fixQuerySql =
                    sqlBuilder.dataBaseType(databaseType).isDivisions(task.isDivisions()).offset(fixStart, fixOffset)
                              .builder();
                log.debug("query table[{}] sql: [{}]", tableMetadata.getTableName(), fixQuerySql);
                takeConnection();
                try (final Stream<RowDataHash> resultStream = jdbcTemplate.queryForStream(fixQuerySql,
                    (RowMapper<RowDataHash>) (rs, rowNum) -> context.resultSetHandler(rs))) {
                    // Push the data to Kafka according to the fragmentation order
                    int recordSize = syncSend(topic, resultStream.collect(Collectors.toList()));
                    queryRowSize.set(recordSize);
                    fixStart = fixStart + fixOffset;
                } catch (DataAccessException exception) {
                    log.error("jdbc query stream [{}] error : {}", fixQuerySql, exception.getMessage());
                    throw new ExtractDataAccessException();
                } finally {
                    releaseConnection();
                    final LocalDateTime end = LocalDateTime.now();
                    log.info("extract table[{}] cost [{}] millis", tableName, Duration.between(start, end).toMillis());
                }
            }
        } catch (Exception ex) {
            log.error("jdbc query stream count latch error [{}] : {}", tableName, ex.getMessage());
            throw new ExtractDataAccessException();
        }
    }

    private void builderQuerySqlByTaskOffset(int[][] taskOffset, TableMetadata tableMetadata,
        List<String> querySqlList) {
        final int taskCount = taskOffset.length;
        final SelectSqlBuilder sqlBuilder = new SelectSqlBuilder(tableMetadata, schema);
        task.setDivisionsTotalNumber(taskCount);
        IntStream.range(0, taskCount).forEach(idx -> {
            final String querySql = sqlBuilder.dataBaseType(databaseType).isDivisions(task.isDivisions())
                                              .offset(taskOffset[idx][0], taskOffset[idx][1]).builder();
            log.debug("query table[{}] sql: [{}]", tableMetadata.getTableName(), querySql);
            querySqlList.add(querySql);
        });
    }

    private void executeTask(int[][] taskOffset, TableMetadata tableMetadata, QueryTableRowContext context) {
        final String tableName = tableMetadata.getTableName();
        final LocalDateTime start = LocalDateTime.now();

        List<String> querySqlList = new ArrayList<>();
        builderQuerySqlByTaskOffset(taskOffset, tableMetadata, querySqlList);
        if (querySqlList.size() == 1) {
            String queryAllRows = querySqlList.get(0);
            takeConnection();
            try (Stream<RowDataHash> resultStream = jdbcTemplate
                .queryForStream(queryAllRows, (RowMapper<RowDataHash>) (rs, rowNum) -> context.resultSetHandler(rs))) {
                // Push the data to Kafka according to the fragmentation order
                syncSend(topic, resultStream.collect(Collectors.toList()));
            } catch (DataAccessException exception) {
                log.error("jdbc query stream [{}] error : {}", queryAllRows, exception.getMessage());
                throw new ExtractDataAccessException();
            } finally {
                releaseConnection();
                final LocalDateTime end = LocalDateTime.now();
                log.info("extract table[{}] cost [{}] millis", tableName, Duration.between(start, end).toMillis());
            }
        }
    }

    private void takeConnection() {
        while (!connectionManager.getConnection()) {
            ThreadUtil.sleep(50);
        }
    }

    private void releaseConnection() {
        connectionManager.releaseConnection();
    }

    /**
     * query table row context
     */
    class QueryTableRowContext {
        private final ResultSetHashHandler resultSetHashHandler = new ResultSetHashHandler();
        private final ResultSetHandlerFactory resultSetFactory = new ResultSetHandlerFactory();
        private ResultSetHandler resultSetHandler;
        private List<String> columns;
        private List<String> primary;

        QueryTableRowContext(TableMetadata tableMetadata, DataBaseType databaseType) {
            this.resultSetHandler = this.resultSetFactory.createHandler(databaseType);
            this.columns = MetaDataUtil.getTableColumns(tableMetadata);
            this.primary = MetaDataUtil.getTablePrimaryColumns(tableMetadata);
        }

        public RowDataHash resultSetHandler(ResultSet rs) {
            return resultSetHashHandler.handler(primary, columns, resultSetHandler.putOneResultSetToMap(rs));
        }
    }
}
