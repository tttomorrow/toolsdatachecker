package org.opengauss.datachecker.extract.task;


import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.*;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.cache.TableExtractStatusCache;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.kafka.KafkaProducerService;
import org.opengauss.datachecker.extract.util.HashHandler;
import org.opengauss.datachecker.extract.util.MetaDataUtil;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.CollectionUtils;

import java.sql.*;
import java.util.*;

/**
 * @author wang chao
 * @description 数据抽取线程类
 * @date 2022/5/12 19:17
 * @since 11
 **/
@Slf4j
public class ExtractTaskThread implements Runnable {

    /**
     * 数据推送Kafka Topic信息
     */
    private final Topic topic;
    /**
     * 当前抽取任务对象
     */
    private final ExtractTask task;
    /**
     * 当前执行端点信息
     */
    private final Endpoint endpoint;
    private final String schema;

    private final JdbcTemplate jdbcTemplate;
    private final KafkaProducerService kafkaProducerService;
    private final CheckingFeignClient checkingFeignClient;


    /**
     * 线程构造函数
     *
     * @param task    Kafka Topic信息
     * @param topic   数据抽取流程编号
     * @param support 线程参数封装
     */
    public ExtractTaskThread(ExtractTask task, Topic topic, ExtractThreadSupport support) {
        this.task = task;
        this.topic = topic;
        this.schema = support.getExtractProperties().getSchema();
        this.endpoint = support.getExtractProperties().getEndpoint();
        this.jdbcTemplate = new JdbcTemplate(support.getDataSourceOne());
        this.kafkaProducerService = support.getKafkaProducerService();
        this.checkingFeignClient = support.getCheckingFeignClient();
    }


    @Override
    public void run() {
        log.info("start extract task={}", task.getTaskName());

        TableMetadata tableMetadata = task.getTableMetadata();

        // 根据当前任务中表元数据信息，构造查询SQL
        String sql = new SelectSqlBulder(tableMetadata, schema, task.getStart(), task.getOffset()).builder();
        // 通过JDBC SQL  查询数据
        List<Map<String, String>> dataRowList = queryColumnValues(sql);
        log.info("query extract task={} completed", task.getTaskName());
        // 对查询出的数据结果 进行哈希计算
        RowDataHashHandler handler = new RowDataHashHandler();
        List<RowDataHash> recordHashList = handler.handlerQueryResult(tableMetadata, dataRowList);
        log.info("hash extract task={} completed", task.getTaskName());
        // 推送本地缓存 根据分片顺序将数据推送到kafka
        String tableName = task.getTableName();
        // 当前分片任务，之前的任务状态未执行完成，请稍后再次检查尝试
        kafkaProducerService.syncSend(topic, recordHashList);
        log.info("send kafka extract task={} completed", task.getTaskName());
        while (task.isDivisions() && !TableExtractStatusCache.checkComplated(tableName, task.getDivisionsOrdinal())) {
            log.debug("task=[{}] wait divisions of before , send data to kafka completed", task.getTaskName());
            ThreadUtil.sleep(100);
        }
        // 推送完成则更新当前任务的抽取状态
        TableExtractStatusCache.update(tableName, task.getDivisionsOrdinal());
        log.info("update extract task={} status completed", task.getTaskName());
        if (!task.isDivisions()) {
            // 通知校验服务，当前表对应任务数据抽取已经完成
            checkingFeignClient.refushTableExtractStatus(tableName, endpoint);
            log.info("refush table extract status tableName={} status completed", task.getTaskName());
        }
        if (task.isDivisions() && task.getDivisionsOrdinal() == task.getDivisionsTotalNumber()) {
            // 当前表的数据抽取任务完成(所有子任务均完成)
            // 通知校验服务，当前表对应任务数据抽取已经完成
            checkingFeignClient.refushTableExtractStatus(tableName, endpoint);
            log.info("refush table=[{}] extract status completed,task=[{}]", tableName, task.getTaskName());
        }
    }

    /**
     * 通过JDBC SQL  查询数据
     *
     * @param sql 执行SQL
     * @return 查询结果
     */
    private List<Map<String, String>> queryColumnValues(String sql) {
        Map<String, Object> map = new HashMap<>();
        // 使用JDBC查询当前任务抽取数据
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(jdbcTemplate);
        // 查询当前任务数据，并对数据进行规整
        return jdbc.query(sql, map, (rs, rowNum) -> {
            // 获取当前结果集对应的元数据信息
            ResultSetMetaData metaData = rs.getMetaData();
            // 结果集处理器
            ResultSetHandler handler = new ResultSetHandler();
            // 查询结果集 根据元数据信息 进行数据转换
            return handler.putOneResultSetToMap(rs, metaData);
        });
    }
}
