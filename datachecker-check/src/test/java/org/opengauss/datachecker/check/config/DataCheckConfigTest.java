package org.opengauss.datachecker.check.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class DataCheckConfigTest {


    @Autowired
    private DataCheckConfig dataCheckConfig;


    @Test
    void testGetCheckResultPaht() {

        final String checkResultPaht = dataCheckConfig.getCheckResultPath();
        System.out.println(checkResultPaht);
    }
}
