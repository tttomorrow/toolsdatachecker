package org.opengauss.datachecker.extract.task;

import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Set;

@SpringBootTest
public class ExtractTaskBuilderTest {

    @Autowired
    private ExtractTaskBuilder extractTaskBuilder;
    @Autowired
    private MetaDataService metadataService;

    @PostConstruct
    public void init() {
        MetaDataCache.initCache();
        MetaDataCache.putMap(metadataService.queryMetaDataOfSchema());
    }

    @Test
    public void builderTest() {
        Set<String> tables = MetaDataCache.getAllKeys();
        List<ExtractTask> extractTasks = extractTaskBuilder.builder(tables);
        for (ExtractTask task : extractTasks) {
            System.out.println(task);
        }
    }
}
