package org.opengauss.datachecker.extract.task;

import lombok.Getter;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.kafka.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;

/**
 * 抽取线程 参数封装
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/30
 * @since ：11
 */
@Getter
@Service
public class ExtractThreadSupport {
    @Autowired
    private DataSource dataSourceOne;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @Autowired
    private CheckingFeignClient checkingFeignClient;

    @Autowired
    private ExtractProperties extractProperties;
}
