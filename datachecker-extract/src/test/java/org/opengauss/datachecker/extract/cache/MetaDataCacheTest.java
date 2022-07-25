package org.opengauss.datachecker.extract.cache;

import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.PostConstruct;


@SpringBootTest
public class MetaDataCacheTest {

    @Autowired
    private MetaDataService metadataService;

    @PostConstruct
    public void init() {
        MetaDataCache.initCache();
        MetaDataCache.putMap(metadataService.queryMetaDataOfSchema());
    }

    @Test
    public void getTest() {
        System.out.println(MetaDataCache.get("client"));
    }

    @Test
    public void getAllKeysTest() {
        System.out.println(MetaDataCache.getAllKeys());
    }

}
