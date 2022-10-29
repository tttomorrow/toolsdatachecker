/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.extract.cache;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * MetaDataCacheTest
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/14
 * @since ：11
 */
@Slf4j
@TestInstance(Lifecycle.PER_CLASS)
@ActiveProfiles("source")
@SpringBootTest
public class MetaDataCacheTest {
    @Autowired
    private JdbcTemplate jdbcTemplateOne;

    @BeforeAll
    private void createTable() {
        createEnv_metacache();
    }

    @AfterAll
    private void cleanEnv() {
        jdbcTemplateOne.execute(CreateTableSql.DROP);
    }

    private void createEnv_metacache() {
        jdbcTemplateOne.execute(CreateTableSql.CREATE);
    }

    @SuppressWarnings("all")
    interface CreateTableSql {
        String DROP = "DROP table if EXISTS  t_data_checker_meta_cache_01; ";
        String CREATE =
            "CREATE TABLE `t_data_checker_meta_cache_01` (`id` INT(10) NOT NULL,PRIMARY KEY (`id`) USING BTREE)ENGINE=InnoDB;";

    }

    /**
     * getTest
     */
    @DisplayName("add table t_data_checker_meta_cache_01")
    @Test
    public void test_add_table_t_data_checker_meta_cache_01() {
        final TableMetadata tableMetadata = MetaDataCache.get("t_data_checker_meta_cache_01");
        assertEquals("t_data_checker_meta_cache_01", tableMetadata.getTableName());
    }

    @DisplayName("remove table t_data_checker_meta_cache_01")
    @Test
    public void test_remove_table_t_data_checker_meta_cache_01() {
        String tableName = "t_data_checker_meta_cache_01";
        TableMetadata tableMetadata = MetaDataCache.get(tableName);
        assertEquals(tableName, tableMetadata.getTableName());
        MetaDataCache.remove(tableName);
        final Set<String> allKeys = MetaDataCache.getAllKeys();
        assertEquals(false, allKeys.contains(tableName));
    }

    /**
     * getAllKeysTest
     */
    @Test
    public void getAllKeysTest() {
        log.info("" + MetaDataCache.getAllKeys());
    }
}
