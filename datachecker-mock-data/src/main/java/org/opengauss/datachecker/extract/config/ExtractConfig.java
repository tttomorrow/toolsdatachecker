package org.opengauss.datachecker.extract.config;

import lombok.Getter;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.DataSourceType;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class ExtractConfig {
    /**
     * schema的初始化值由配置文件加载
     */
    private final String schema = "test";

    /**
     * schema的初始化值由配置文件加载
     */
    private final DataSourceType dataSourceType = DataSourceType.Source;

    /**
     * schema的初始化值由配置文件加载
     */
    private final DataBaseType databaseType = DataBaseType.MS;
}
