package org.opengauss.datachecker.extract.task;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.util.IdGenerator;
import org.opengauss.datachecker.common.util.TopicUtil;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.util.MetaDataUtil;
import org.opengauss.datachecker.extract.util.TestJsonUtil;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.kafka.core.KafkaTemplate;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.opengauss.datachecker.extract.util.TestJsonUtil.KEY_META_DATA_13_TABLE;

@ExtendWith(MockitoExtension.class)
class ExtractTaskRunnableTest {
    @Mock
    private ExtractThreadSupport mockSupport;
    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;
    private ExtractTaskRunnable extractTaskRunnableUnderTest;

    @BeforeAll
    static void setUp() {
        HashMap<String, TableMetadata> result = TestJsonUtil.parseHashMap(KEY_META_DATA_13_TABLE, TableMetadata.class);
        MetaDataCache.putMap(result);

    }

    @DisplayName("extract mysql ")
    void testRun() throws SQLException, NoSuchFieldException, IllegalAccessException {
        ExtractProperties properties = mockExtractProperties();
        when(mockSupport.getExtractProperties()).thenReturn(properties);
        DataSource datasource = Mockito.mock(DataSource.class);
        when(mockSupport.getDataSourceOne()).thenReturn(datasource);
        CheckingFeignClient checkingFeignClient = Mockito.mock(CheckingFeignClient.class);
        when(mockSupport.getCheckingFeignClient()).thenReturn(checkingFeignClient);
        when(mockSupport.getKafkaTemplate()).thenReturn(kafkaTemplate);
        final Connection connection = Mockito.mock(Connection.class);
        when(datasource.getConnection()).thenReturn(connection);
        when(datasource.getConnection().createStatement()).thenReturn(Mockito.mock(Statement.class));

        String tableName = "t_data_checker_0033_02";
        ExtractTask task = mockExtractTask(tableName);
        Topic topic = mockTopic(tableName, properties.getEndpoint());
        extractTaskRunnableUnderTest = new ExtractTaskRunnable(task, topic, mockSupport);
        String querySql = "SELECT `d1`,`d2`,`d3`,`d4`,`d5` FROM `test`.`t_data_checker_0033_02`";
        mockJdbcTemplateQueryStream(tableName, querySql, properties.getDatabaseType());
        extractTaskRunnableUnderTest.run();
    }

    private void mockJdbcTemplateQueryStream(String tableName, String querySql, DataBaseType databaseType)
        throws NoSuchFieldException, IllegalAccessException {
        final Field jdbcTemplateField = extractTaskRunnableUnderTest.getClass().getDeclaredField("jdbcTemplate");
        jdbcTemplateField.setAccessible(true);
        ResultSetHashHandler resultSetHashHandler = new ResultSetHashHandler();
        ResultSetHandlerFactory resultSetFactory = new ResultSetHandlerFactory();
        ResultSetHandler resultSetHandler = resultSetFactory.createHandler(databaseType);
        final TableMetadata tableMetadata = MetaDataCache.get(tableName);
        List<String> columns = MetaDataUtil.getTableColumns(tableMetadata);
        List<String> primary = MetaDataUtil.getTablePrimaryColumns(tableMetadata);
        final JdbcTemplate jdbcTemplate = (JdbcTemplate) jdbcTemplateField.get(extractTaskRunnableUnderTest);
        Mockito.when(jdbcTemplate.queryForStream(querySql, (RowMapper<RowDataHash>) (rs, rowNum) -> resultSetHashHandler
            .handler(primary, columns, resultSetHandler.putOneResultSetToMap(rs)))).thenAnswer((invocation) -> {
            //                   RowMapper<RowDataHash> resultSetExtractor = (RowMapper<RowDataHash>) invocation.getArgument(1);
            ResultSet rs = Mockito.mock(ResultSet.class);
            // two times it returns true and third time returns false.
            when(rs.next()).thenReturn(true, true, false);
            // Mock ResultSet to return two rows.
            Mockito.when(rs.getInt(ArgumentMatchers.eq("ID"))).thenReturn(506, 400);
            Mockito.when(rs.getString(ArgumentMatchers.eq("NAME"))).thenReturn("Jim Carrey", "John Travolta");
            Mockito.when(rs.getBoolean(ArgumentMatchers.eq("STATUS"))).thenReturn(true, false);
            return resultSetHashHandler.handler(primary, columns, resultSetHandler.putOneResultSetToMap(rs));
            //                   return resultSetExtractor.mapRow(rs, 1);
        });
    }

    private ExtractProperties mockExtractProperties() {
        ExtractProperties properties = new ExtractProperties();
        properties.setDatabaseType(DataBaseType.MS);
        properties.setEndpoint(Endpoint.SOURCE);
        properties.setSchema("test");
        return properties;
    }

    private Topic mockTopic(String tableName, Endpoint endpoint) {
        Topic topic = new Topic();
        topic.setTableName(tableName);
        topic.setPartitions(1);
        topic.setTopicName(TopicUtil.buildTopicName(IdGenerator.nextId36(), endpoint, tableName));
        return topic;
    }

    private ExtractTask mockExtractTask(String tableName) {
        final TableMetadata tableMetadata = MetaDataCache.get(tableName);
        ExtractTask task = new ExtractTask();
        task.setTableName(tableName);
        task.setTaskName("task_" + tableName);
        task.setStart(0);
        task.setOffset(tableMetadata.getTableRows());
        task.setTableMetadata(tableMetadata);
        task.setDivisionsOrdinal(1);
        task.setDivisionsOrdinal(1);
        return task;
    }
}
