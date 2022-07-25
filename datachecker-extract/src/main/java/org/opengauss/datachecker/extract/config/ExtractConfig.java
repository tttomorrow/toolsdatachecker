package org.opengauss.datachecker.extract.config;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/23
 * @since ：11
 */
@Slf4j
@Component
public class ExtractConfig {

    @Autowired
    private ExtractProperties extractProperties;

    @PostConstruct
    public void initLoad() {
        log.info("check config properties [{}]", JsonObjectUtil.format(extractProperties));
    }

}
