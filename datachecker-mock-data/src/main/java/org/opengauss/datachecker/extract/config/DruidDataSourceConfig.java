package org.opengauss.datachecker.extract.config;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.support.http.StatViewServlet;
import com.alibaba.druid.support.http.WebStatFilter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class DruidDataSourceConfig {

    /**
     * <pre>
     *  将自定义的 Druid数据源添加到容器中，不再让 Spring Boot 自动创建
     *  绑定全局配置文件中的 druid 数据源属性到 com.alibaba.druid.pool.DruidDataSource从而让它们生效
     *  @ConfigurationProperties(prefix = "spring.datasource")：作用就是将 全局配置文件中
     *  前缀为 spring.datasource的属性值注入到 com.alibaba.druid.pool.DruidDataSource 的同名参数中
     *  </pre>
     *
     * @return
     */
    @Primary
    @Bean("dataSourceOne")
    @ConfigurationProperties(prefix = "spring.datasource.druid")
    public DataSource druidDataSourceOne() {
        return new DruidDataSource();
    }


    @Bean("jdbcTemplateOne")
    public JdbcTemplate jdbcTemplateOne(@Qualifier("dataSourceOne") DataSource dataSourceOne) {
        return new JdbcTemplate(dataSourceOne);
    }

}
