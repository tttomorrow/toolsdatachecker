package org.opengauss.datachecker.extract.task;


import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.*;
import org.opengauss.datachecker.common.exception.ExtractException;
import org.opengauss.datachecker.extract.cache.TableExtractStatusCache;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.dml.DmlBuilder;
import org.opengauss.datachecker.extract.dml.SelectDmlBuilder;
import org.opengauss.datachecker.extract.kafka.KafkaProducerService;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.CollectionUtils;

import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * @author wang chao
 * @description 数据抽取线程类
 * @date 2022/5/12 19:17
 * @since 11
 **/
@Slf4j
public class IncrementExtractTaskThread implements Runnable {

    /**
     * SQL 单次查询语句，构建查询参数最大个数
     */
    private static final int MAX_QUERY_ROW_COUNT = 1000;

    private final Topic topic;
    private final String schema;
    private final String taskName;
    private final String tableName;
    private final Endpoint endpoint;
    private final SourceDataLog sourceDataLog;
    private final JdbcTemplate jdbcTemplate;
    private final KafkaProducerService kafkaProducerService;
    private final CheckingFeignClient checkingFeignClient;
    private final MetaDataService metaDataService;

    private boolean singlePrimaryKey;

    /**
     * 线程构造函数
     *
     * @param task    Kafka Topic信息
     * @param topic   数据抽取流程编号
     * @param support 线程参数封装
     */
    public IncrementExtractTaskThread(ExtractIncrementTask task, Topic topic, IncrementExtractThreadSupport support) {
        this.topic = topic;
        this.schema = support.getExtractProperties().getSchema();
        this.endpoint = support.getExtractProperties().getEndpoint();
        this.tableName = task.getTableName();
        this.taskName = task.getTaskName();
        this.sourceDataLog = task.getSourceDataLog();
        this.jdbcTemplate = new JdbcTemplate(support.getDataSourceOne());
        this.kafkaProducerService = support.getKafkaProducerService();
        this.checkingFeignClient = support.getCheckingFeignClient();
        this.metaDataService = support.getMetaDataService();
    }


    @Override
    public void run() {
        log.info("start extract task={}", taskName);
        TableMetadata tableMetadata = getTableMetadata();

        // 根据当前任务中表元数据信息，构造查询SQL
        SelectDmlBuilder sqlBuilder = buildSelectSql(tableMetadata, schema);

        // 查询当前任务数据，并对数据进行规整
        HashMap<String, Object> paramMap = new HashMap<>();
        final List<String> compositePrimaryValues = sourceDataLog.getCompositePrimaryValues();
        paramMap.put(DmlBuilder.PRIMARY_KEYS, getSqlParam(sqlBuilder, tableMetadata.getPrimaryMetas(), compositePrimaryValues));

        // 查询当前任务数据，并对数据进行规整
        List<Map<String, String>> dataRowList = queryColumnValues(sqlBuilder.build(), paramMap);
        log.info("query extract task={} completed row count=[{}]", taskName, dataRowList.size());
        // 对查询出的数据结果 进行哈希计算
        RowDataHashHandler handler = new RowDataHashHandler();
        List<RowDataHash> recordHashList = handler.handlerQueryResult(tableMetadata, dataRowList);
        log.info("hash extract task={} completed", taskName);
        // 推送本地缓存 根据分片顺序将数据推送到kafka
        kafkaProducerService.syncSend(topic, recordHashList);
        log.info("send kafka extract task={} completed", taskName);
        // 推送完成则更新当前任务的抽取状态
        TableExtractStatusCache.update(tableName, 1);
        log.info("update extract task={} status completed", tableName);
        // 通知校验服务，当前表对应任务数据抽取已经完成
        checkingFeignClient.refushTableExtractStatus(tableName, endpoint);
        log.info("refush table extract status tableName={} status completed", tableName);

    }

    /**
     *    查询SQL构建后期优化
     * 查询SQL 构建 select colums from table where pk in(...) <p>
     * 后期优化方式：<p>
     * 单主键方式
     * SELECT *
     * FROM (
     * SELECT '14225351881572354' cid UNION ALL
     * SELECT '14225351898349591' UNION ALL
     * SELECT '14225351902543878'
     * ) AS tmp, test.test1  t
     * WHERE tmp.cid = t.b_number; <p>
     * <p>
     * 复合主键方式
     * SELECT *
     * FROM (
     * SELECT '1523567590573785088' cid,'type_01' ctype UNION ALL
     * SELECT '1523567590573785188','type_01' UNION ALL
     * SELECT '1523567590573785189','type_03'
     * ) AS tmp, test.test2  t
     * WHERE tmp.cid = t.b_number AND tmp.ctype=t.b_type;
     *
     * @param tableMetadata 表元数据信息
     * @param schema        数据库schema
     * @return SQL构建器对象
     */
    private SelectDmlBuilder buildSelectSql(TableMetadata tableMetadata, String schema) {
        // 复合主键表数据查询
        SelectDmlBuilder dmlBuilder = new SelectDmlBuilder();
        final List<ColumnsMetaData> primaryMetas = tableMetadata.getPrimaryMetas();
        if (singlePrimaryKey) {
            final ColumnsMetaData primaryData = primaryMetas.get(0);
            dmlBuilder.schema(schema)
                    .columns(tableMetadata.getColumnsMetas())
                    .tableName(tableMetadata.getTableName())
                    .conditionPrimary(primaryData);
        } else {
            // 复合主键表数据查询
            dmlBuilder.schema(schema)
                    .columns(tableMetadata.getColumnsMetas())
                    .tableName(tableMetadata.getTableName())
                    .conditionCompositePrimary(primaryMetas);
        }
        return dmlBuilder;
    }

    /**
     * 构建JDBC 查询参数
     *
     * @param sqlBuilder             SQL构建器
     * @param primaryMetas           主键信息
     * @param compositePrimaryValues 查询参数
     * @return 封装后的JDBC查询参数
     */
    private List getSqlParam(SelectDmlBuilder sqlBuilder, List<ColumnsMetaData> primaryMetas, List<String> compositePrimaryValues) {
        if (singlePrimaryKey) {
            return compositePrimaryValues;
        } else {
            return sqlBuilder.conditionCompositePrimaryValue(primaryMetas, compositePrimaryValues);
        }
    }

    /**
     * 主键表数据查询
     *
     * @param selectDml 查询SQL
     * @param paramMap  查询参数
     * @return 查询结果
     */
    private List<Map<String, String>> queryColumnValues(String selectDml, Map<String, Object> paramMap) {
        // 使用JDBC查询当前任务抽取数据
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(jdbcTemplate);
        return jdbc.query(selectDml, paramMap, (rs, rowNum) -> {
            // 获取当前结果集对应的元数据信息
            ResultSetMetaData metaData = rs.getMetaData();
            // 结果集处理器
            ResultSetHandler handler = new ResultSetHandler();
            // 查询结果集 根据元数据信息 进行数据转换
            return handler.putOneResultSetToMap(rs, metaData);
        });
    }

    private TableMetadata getTableMetadata() {
        final TableMetadata metadata = metaDataService.queryMetaDataOfSchema(tableName);
        if (Objects.isNull(metadata) || CollectionUtils.isEmpty(metadata.getPrimaryMetas())) {
            throw new ExtractException(tableName + " metadata not found！");
        }
        this.singlePrimaryKey = metadata.getPrimaryMetas().size() == 1;
        return metadata;
    }
}
