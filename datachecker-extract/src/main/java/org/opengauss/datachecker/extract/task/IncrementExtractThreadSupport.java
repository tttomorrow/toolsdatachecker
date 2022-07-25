package org.opengauss.datachecker.extract.task;

import lombok.Getter;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 抽取线程 参数封装
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/30
 * @since ：11
 */
@Getter
@Service
public class IncrementExtractThreadSupport extends ExtractThreadSupport {

    @Autowired
    private MetaDataService metaDataService;
}
