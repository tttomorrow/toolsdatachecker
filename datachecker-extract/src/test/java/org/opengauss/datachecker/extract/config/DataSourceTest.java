package org.opengauss.datachecker.extract.config;

import com.alibaba.druid.pool.DruidDataSource;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.extract.ExtractApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@SpringBootTest (classes = ExtractApplication.class)
public class DataSourceTest {

    @Autowired
    private ApplicationContext applicationContext;


    @Test
    public void contextLoadTest() throws SQLException {
        DruidDataSource dataSourceOne = (DruidDataSource)applicationContext.getBean("dataSourceOne");
//        DruidDataSource dataSourceTwo = (DruidDataSource)applicationContext.getBean("dataSourceTwo");
//        DruidDataSource dataSourceThree = (DruidDataSource)applicationContext.getBean("dataSourceThree");

        System.out.println("dataSourceOne " + dataSourceOne.getClass());
        System.out.println("dataSourceOne " + dataSourceOne.getConnection());
        System.out.println("druid dataSourceOne 最大连接数 ：" + dataSourceOne.getMaxActive());
        System.out.println("druid dataSourceOne 最大初始化连接数 ：" + dataSourceOne.getInitialSize());

        System.out.println(" =========================================== ");
//
//        System.out.println("dataSourceTwo " + dataSourceTwo.getClass());
//        System.out.println("dataSourceTwo " + dataSourceTwo.getConnection());
//        System.out.println("druid dataSourceTwo 最大连接数 ：" + dataSourceTwo.getMaxActive());
//        System.out.println("druid dataSourceTwo 最大初始化连接数 ：" + dataSourceTwo.getInitialSize());
//
//        System.out.println(" =========================================== ");
//
//        System.out.println("dataSourceThree " + dataSourceThree.getClass());
//        System.out.println("dataSourceThree " + dataSourceThree.getConnection());
//        System.out.println("druid dataSourceThree 最大连接数 ：" + dataSourceThree.getMaxActive());
//        System.out.println("druid dataSourceThree 最大初始化连接数 ：" + dataSourceThree.getInitialSize());

        dataSourceOne.close();
//        dataSourceTwo.close();
//        dataSourceThree.close();

    }

    @Test
    public void JdbcTemplateTest() {

        JdbcTemplate JdbcTemplateOne = (JdbcTemplate)applicationContext.getBean("JdbcTemplateOne");
//        JdbcTemplate JdbcTemplateTwo = (JdbcTemplate)applicationContext.getBean("JdbcTemplateTwo");
//        JdbcTemplate dataSourceThree = (JdbcTemplate)applicationContext.getBean("JdbcTemplateThree");
//
//        List<Map<String, Object>> listTwo = JdbcTemplateTwo.queryForList("select * from client");
//        for (Map<String, Object> map : listTwo) {
//            System.out.println(map);
//        }
//
//        System.out.println("======================================================================================");
//        List<Map<String, Object>> listThree = dataSourceThree.queryForList("select * from client");
//        for (Map<String, Object> map : listThree) {
//            System.out.println(map);
//        }
    }
}
