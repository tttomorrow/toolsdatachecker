package org.opengauss.datachecker.check.config;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.io.File;


/**
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Slf4j
@Component
public class DataCheckConfig {

    private static final String CHECK_RESULT_PATH = File.separator + "Result" + File.separator + "Date" + File.separator;

    @Autowired
    private DataCheckProperties properties;

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @PostConstruct
    public DataCheckProperties getDataCheckProperties() {
        log.info("check config properties [{}]", JsonObjectUtil.format(properties));
        return properties;
    }

    public int getBucketCapacity() {
        return properties.getBucketExpectCapacity();
    }

    public String getCheckResultPath() {
        return properties.getDataPath() + CHECK_RESULT_PATH;
    }
}
