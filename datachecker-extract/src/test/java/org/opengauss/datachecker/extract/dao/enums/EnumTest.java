package org.opengauss.datachecker.extract.dao.enums;

import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.enums.DataSourceType;
import org.opengauss.datachecker.common.util.EnumUtil;
import org.springframework.boot.test.context.SpringBootTest;


@SpringBootTest
public class EnumTest {

    @Test
    void testEnum()  {
        DataSourceType type = DataSourceType.Sink;

        System.out.println(type);
        System.out.println(type.equals(DataSourceType.valueOf("Sink")));
        System.out.println(EnumUtil.valueOfIgnoreCase(DataSourceType.class,"Sinkl"));

        System.out.println(EnumUtil.valueOf(DataSourceType.class,"Sinkl"));
        System.out.println(EnumUtil.valueOf(DataSourceType.class,"Sink"));

        System.out.println(EnumUtil.valueOf(DataSourceType.class,"sink"));

        System.out.println(EnumUtil.valueOfIgnoreCase(DataSourceType.class,"sink"));
    }
}
