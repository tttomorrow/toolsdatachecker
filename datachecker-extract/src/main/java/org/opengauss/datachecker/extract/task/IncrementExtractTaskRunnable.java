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
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.ExtractIncrementTask;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.exception.ExtractException;
import org.opengauss.datachecker.extract.cache.TableExtractStatusCache;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.dml.DmlBuilder;
import org.opengauss.datachecker.extract.dml.SelectDmlBuilder;
import org.opengauss.datachecker.extract.kafka.KafkaProducerWapper;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.opengauss.datachecker.extract.util.MetaDataUtil;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Incremental data extraction thread class
 *
 * @author wang chao
 * @date 2022/5/12 19:17
 * @since 11
 **/
@Slf4j
public class IncrementExtractTaskRunnable extends KafkaProducerWapper implements Runnable {
    private final Topic topic;
    private final String schema;
    private final String taskName;
    private final String tableName;
    private final Endpoint endpoint;
    private final DataBaseType databaseType;
    private final SourceDataLog sourceDataLog;
    private final JdbcTemplate jdbcTemplate;
    private final CheckingFeignClient checkingFeignClient;
    private final MetaDataService metaDataService;
    private final ResultSetHandlerFactory resultSetFactory;

    private boolean isSinglePrimaryKey;

    /**
     * IncrementExtractTaskRunnable
     *
     * @param task    task
     * @param topic   topic
     * @param support support
     */
    public IncrementExtractTaskRunnable(ExtractIncrementTask task, Topic topic, IncrementExtractThreadSupport support) {
        super(support.getKafkaTemplate());
        this.topic = topic;
        schema = support.getExtractProperties().getSchema();
        endpoint = support.getExtractProperties().getEndpoint();
        databaseType = support.getExtractProperties().getDatabaseType();
        tableName = task.getTableName();
        taskName = task.getTaskName();
        sourceDataLog = task.getSourceDataLog();
        jdbcTemplate = new JdbcTemplate(support.getDataSourceOne());
        checkingFeignClient = support.getCheckingFeignClient();
        metaDataService = support.getMetaDataService();
        resultSetFactory = new ResultSetHandlerFactory();
    }

    @Override
    public void run() {
        log.info("start extract task={}", taskName);
        TableMetadata tableMetadata = getTableMetadata();

        // Construct query SQL according to the metadata information of the table in the current task
        SelectDmlBuilder sqlBuilder = buildSelectSql(tableMetadata, schema);

        // Query the current task data and organize the data
        HashMap<String, Object> paramMap = new HashMap<>(InitialCapacity.CAPACITY_16);
        final List<String> compositePrimaryValues = sourceDataLog.getCompositePrimaryValues();
        paramMap.put(DmlBuilder.PRIMARY_KEYS,
            getSqlParam(sqlBuilder, tableMetadata.getPrimaryMetas(), compositePrimaryValues));

        // Query the current task data and organize the data && Hash the queried data results
        List<RowDataHash> dataRowList = queryColumnValues(sqlBuilder.build(), paramMap, tableMetadata);
        log.info("query extract task={} completed row count=[{}]", taskName, dataRowList.size());
        // Push the local cache to push the data to Kafka according to the fragmentation order
        syncSend(topic, dataRowList);
        log.info("send kafka extract task={} completed", taskName);
        // When the push is completed, the extraction status of the current task will be updated
        TableExtractStatusCache.update(tableName, 1);
        log.info("update extract task={} status completed", tableName);
        // Notify the verification service that the task data extraction corresponding to
        // the current table has been completed
        checkingFeignClient.refreshTableExtractStatus(tableName, endpoint, endpoint.getCode());
        log.info("refush table extract status tableName={} status completed", tableName);
    }

    /**
     * Query SQL build post optimization
     * Query SQL build select colums from table where pk in(...) <p>
     * Post optimization method：<p>
     * Single primary key type
     * SELECT *
     * FROM (
     * SELECT '14225351881572354' cid UNION ALL
     * SELECT '14225351898349591' UNION ALL
     * SELECT '14225351902543878'
     * ) AS tmp, test.test1  t
     * WHERE tmp.cid = t.b_number; <p>
     * <p>
     * Composite primary key type
     * SELECT *
     * FROM (
     * SELECT '1523567590573785088' cid,'type_01' ctype UNION ALL
     * SELECT '1523567590573785188','type_01' UNION ALL
     * SELECT '1523567590573785189','type_03'
     * ) AS tmp, test.test2  t
     * WHERE tmp.cid = t.b_number AND tmp.ctype=t.b_type;
     *
     * @param tableMetadata Table metadata information
     * @param schema        Database schema
     * @return SQL builder object
     */
    private SelectDmlBuilder buildSelectSql(TableMetadata tableMetadata, String schema) {
        // Compound primary key table data query
        SelectDmlBuilder dmlBuilder = new SelectDmlBuilder();
        final List<ColumnsMetaData> primaryMetas = tableMetadata.getPrimaryMetas();
        if (isSinglePrimaryKey) {
            final ColumnsMetaData primaryData = primaryMetas.get(0);
            dmlBuilder.schema(schema).columns(tableMetadata.getColumnsMetas()).tableName(tableMetadata.getTableName())
                      .conditionPrimary(primaryData);
        } else {
            // Compound primary key table data query
            dmlBuilder.schema(schema).columns(tableMetadata.getColumnsMetas()).tableName(tableMetadata.getTableName())
                      .conditionCompositePrimary(primaryMetas);
        }
        dmlBuilder.dataBaseType(databaseType);
        return dmlBuilder;
    }

    /**
     * Build JDBC query parameters
     *
     * @param sqlBuilder             SQL builder
     * @param primaryMetas           Primary key information
     * @param compositePrimaryValues Query Parameter
     * @return Encapsulated JDBC query parameters
     */
    private List getSqlParam(SelectDmlBuilder sqlBuilder, List<ColumnsMetaData> primaryMetas,
        List<String> compositePrimaryValues) {
        if (isSinglePrimaryKey) {
            return compositePrimaryValues;
        } else {
            return sqlBuilder.conditionCompositePrimaryValue(primaryMetas, compositePrimaryValues);
        }
    }

    /**
     * Primary key table data query
     *
     * @param selectDml     Query SQL
     * @param paramMap      Query Parameter
     * @param tableMetadata tableMetadata
     * @return query results
     */
    private List<RowDataHash> queryColumnValues(String selectDml, Map<String, Object> paramMap,
        TableMetadata tableMetadata) {
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(jdbcTemplate);
        List<String> columns = MetaDataUtil.getTableColumns(tableMetadata);
        List<String> primary = MetaDataUtil.getTablePrimaryColumns(tableMetadata);
        ResultSetHashHandler resultSetHashHandler = new ResultSetHashHandler();
        ResultSetHandler resultSetHandler = resultSetFactory.createHandler(databaseType);
        return jdbc.query(selectDml, paramMap,
            (rs, rowNum) -> resultSetHashHandler.handler(primary, columns, resultSetHandler.putOneResultSetToMap(rs)));
    }

    private TableMetadata getTableMetadata() {
        final TableMetadata metadata = metaDataService.queryMetaDataOfSchema(tableName);
        if (Objects.isNull(metadata) || CollectionUtils.isEmpty(metadata.getPrimaryMetas())) {
            throw new ExtractException(tableName + " metadata not found！");
        }
        isSinglePrimaryKey = metadata.getPrimaryMetas().size() == 1;
        return metadata;
    }
}
