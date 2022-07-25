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

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class DruidDataSourceConfig {

    /**
     * <pre>
     *  Add custom Druid data sources to the container,no longer let Spring boot automatically create them.
     *  Bind the druid data source adttributes in the global configuration file to the com.alibaba.druid.pool.DruidDataSource
     *  to make them take effect.
     *  {@code @ConfigurationProperties(prefix="spring.datasource.druid.datasourceone")}: Injects the attribute value
     *  prefixed with spring.datasource in the global configuration file to the com.alibaba.druid.pool.DruidDataSource
     *  parameter with the same name.
     *  </pre>
     *
     * @return
     */
    @Bean("dataSourceOne")
    @ConfigurationProperties(prefix = "spring.datasource.druid.datasourceone")
    public DataSource druidDataSourceOne() {
        return new DruidDataSource();
    }


    @Bean("jdbcTemplateOne")
    public JdbcTemplate jdbcTemplateOne(@Qualifier("dataSourceOne") DataSource dataSourceOne) {
        return new JdbcTemplate(dataSourceOne);
    }

    /**
     * Background monitoring
     * Configure the servlet of the Druid monitoring management background.
     * There is no web.xml file when the servlet container is built in. Therefore ,the servlet registration mode of
     * Spring Boot is used.
     * Startup access address : http://localhost:8080/druid/api.html
     *
     * @return return ServletRegistrationBean
     */
    @Bean
    public ServletRegistrationBean<StatViewServlet> initServletRegistrationBean() {

        ServletRegistrationBean<StatViewServlet> bean =
                new ServletRegistrationBean<>(new StatViewServlet(), "/druid/*");
        // Configuring the account and password
        HashMap<String, String> initParameters = new HashMap<>();
        // Add configuration
        // the login key is a fixed loginUsername loginPassword
        initParameters.put("loginUsername", "admin");
        initParameters.put("loginPassword", "123456");

        // if the second parameter is empty,everyone can access it.
        initParameters.put("allow", "");

        // Setting initialization parameters
        bean.setInitParameters(initParameters);
        return bean;
    }

    /**
     * Configuring the filter for druid monitoring - web monitoring
     * WebStatFilter: used to configure management association monitoring statistice between web and druid data sources.
     *
     * @return return FilterRegistrationBean
     */
    @Bean
    public FilterRegistrationBean webStatFilter() {
        FilterRegistrationBean bean = new FilterRegistrationBean();
        bean.setFilter(new WebStatFilter());

        //exclusions: sets the requests to be filtered out so that statistics are not collected.
        Map<String, String> initParams = new HashMap<>();
        // this things don't count.
        initParams.put("exclusions", "*.js,*.css,/druid/*,/jdbc/*");
        bean.setInitParameters(initParams);

        //"/*" indicates that all requests are filtered.
        bean.setUrlPatterns(Arrays.asList("/*"));
        return bean;
    }
}
