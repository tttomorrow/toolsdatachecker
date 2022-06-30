package org.opengauss.datachecker.extract.config;

import com.alibaba.fastjson.annotation.JSONType;
import lombok.Data;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
@Data
@Component
@ConfigurationProperties(prefix = "spring.extract")
@JSONType(orders = {"schema", "databaseType", "endpoint", "debeziumTopic", "debeziumTables"})
public class ExtractProperties {

    /**
     * Database instance : database instance configuration,which cannot be empty
     */
    @NotNull(message = "database instance configuration, cannot be empty")
    private String schema;

    /**
     * Database type: the database type configuration cannot be empty
     */
    @NotNull(message = DataBaseType.API_DESCRIPTION + ",database type configuration, cannot be empty")
    private DataBaseType databaseType;

    /**
     * Endpoint type: Endpoint type,which cannot be empty
     */
    @NotNull(message = Endpoint.API_DESCRIPTION + ",endpoint type configuration, cannot be empty")
    private Endpoint endpoint;

    /**
     * Indicates whether to enable incremental debezium configuration.
     * By default,this function is disabled. the default value is false.
     */
    @NotNull(message = "whether to enable debezium configuration, which cannot be empty")
    private Boolean debeziumEnable;
    /**
     * Debezium incremental migration verification topic.
     * Debezium listens to incremental data in tables and uses a single topic for incremental data management.
     * The debezium incremental verification configuration is not checked here.
     * If the incremental verification service is required,you need to manually configure it.
     */
    private String debeziumTopic;
    /**
     * Group parameter setting for the debezium incremental migration verification topic
     */
    private String debeziumGroupId;

    /**
     * Partitions parameter setting for the debezium incremental migration verification topic
     */
    private int debeziumTopicPartitions = 1;
    /**
     * incremental migration table name list
     */
    private List<String> debeziumTables;
    /**
     * debezium incremental migration verification period: 24 x 60 (unit:minute)
     */
    private int debeziumTimePeriod;
    /**
     * debezium incremental migration verification statistics incremental change record count threshold.
     * the threshold must be greater than 100.
     */
    private int debeziumNumPeriod = 1000;
}
