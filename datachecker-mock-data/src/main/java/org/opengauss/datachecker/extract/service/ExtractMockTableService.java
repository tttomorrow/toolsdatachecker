package org.opengauss.datachecker.extract.service;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.TableRowCount;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wang chao
 * @date 2022/5/8 19:27
 * @since 11
 **/
@Service
@Slf4j
public class ExtractMockTableService {
    @Autowired
    protected JdbcTemplate jdbcTemplateOne;

    @Autowired
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    /**
     * 自动创建指定表
     *
     * @param tableName 创建表
     * @return
     * @throws Exception 目前对于表名重复未做处理，表名重复这里直接抛出异常信息
     */
    public String createTable(String tableName) throws Exception {
        jdbcTemplateOne.execute(MockMapper.CREATE.replace(":TABLENAME", tableName));
        return tableName;
    }

    private static final String SQL_QUERY_TABLE = "SELECT table_name from information_schema.TABLES WHERE table_schema='test' ";

    public List<TableRowCount> getAllTableCount() {
        long start = System.currentTimeMillis();
        List<TableRowCount> tableRowCountList = new ArrayList<>();
        final List<String> tableNameList = jdbcTemplateOne.queryForList(SQL_QUERY_TABLE, String.class);
        if (CollectionUtils.isEmpty(tableNameList)) {
            return new ArrayList<>();
        }
        String sqlQueryTableRowCount = "select count(1) rowCount from test.%s";
        tableNameList.stream().forEach(tableName -> {
            threadPoolTaskExecutor.submit(() -> {
                final Long rowCount = jdbcTemplateOne.queryForObject(String.format(sqlQueryTableRowCount, tableName), Long.class);
                tableRowCountList.add(new TableRowCount(tableName, rowCount));
            });
        });

        while (tableRowCountList.size() != tableNameList.size()) {
            ThreadUtil.sleep(10);
        }

        final long sum = tableRowCountList.stream().mapToLong(TableRowCount::getCount).sum();
        tableRowCountList.add(new TableRowCount("all_table_total", sum));
        tableRowCountList.sort((o1, o2) -> (int) (o1.getCount() - o2.getCount()));

        long end = System.currentTimeMillis();

        System.out.println(" query cost time =" + (end - start) + " sec");
        return tableRowCountList;
    }

    /**
     * 构建创建表SQL语句
     */
    interface MockMapper {
        String CREATE = "CREATE TABLE :TABLENAME (\n" +
                "\t b_number VARCHAR(30) NOT NULL COLLATE 'utf8mb4_0900_ai_ci',\n" +
                "\t b_type VARCHAR(20) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',\n" +
                "\t b_user VARCHAR(20) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',\n" +
                "\t b_int INT(10) NULL DEFAULT NULL,\n" +
                "\t b_bigint BIGINT(19) NULL DEFAULT '0',\n" +
                "\t b_text TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',\n" +
                "\t b_longtext LONGTEXT NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',\n" +
                "\t b_date DATE NULL DEFAULT NULL,\n" +
                "\t b_datetime DATETIME NULL DEFAULT NULL,\n" +
                "\t b_timestamp TIMESTAMP NULL DEFAULT NULL,\n" +
                "\t b_attr1 VARCHAR(255) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',\n" +
                "\t b_attr2 VARCHAR(255) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',\n" +
                "\t b_attr3 VARCHAR(255) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',\n" +
                "\t b_attr4 VARCHAR(255) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',\n" +
                "\t b_attr5 VARCHAR(255) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',\n" +
                "\t b_attr6 VARCHAR(255) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',\n" +
                "\t b_attr7 VARCHAR(255) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',\n" +
                "\t b_attr8 VARCHAR(255) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',\n" +
                "\t b_attr9 VARCHAR(255) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',\n" +
                "\t b_attr10 VARCHAR(255) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',\n" +
                "\tPRIMARY KEY (`b_number`) USING BTREE\n" +
                ")\n" +
                " COLLATE='utf8mb4_0900_ai_ci'\n" +
                " ENGINE=InnoDB ;\n";

    }
}
