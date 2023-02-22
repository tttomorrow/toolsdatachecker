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

package org.opengauss.datachecker.extract.dao;

import org.opengauss.datachecker.common.entry.enums.DataBaseMeta;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;

/**
 * MetaSqlMapper
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/24
 * @since ：11
 */
public class MetaSqlMapper {
    private static final Map<DataBaseType, Map<DataBaseMeta, String>> DATABASE_META_MAPPER = new HashMap<>();

    static {
        Map<DataBaseMeta, String> dataBaseMySql = new HashMap<>();
        dataBaseMySql.put(DataBaseMeta.TABLE, DataBaseMySql.TABLE_METADATA_SQL);
        dataBaseMySql.put(DataBaseMeta.COLUMN, DataBaseMySql.TABLES_COLUMN_META_DATA_SQL);
        dataBaseMySql.put(DataBaseMeta.HEALTH, DataBaseMySql.HEALTH_SQL);
        DATABASE_META_MAPPER.put(DataBaseType.MS, dataBaseMySql);
        Map<DataBaseMeta, String> dataBaseOpenGauss = new HashMap<>();
        dataBaseOpenGauss.put(DataBaseMeta.TABLE, DataBaseOpenGauss.TABLE_METADATA_SQL);
        dataBaseOpenGauss.put(DataBaseMeta.COLUMN, DataBaseOpenGauss.TABLES_COLUMN_META_DATA_SQL);
        dataBaseOpenGauss.put(DataBaseMeta.HEALTH, DataBaseMySql.HEALTH_SQL);
        DATABASE_META_MAPPER.put(DataBaseType.OG, dataBaseOpenGauss);
        Map<DataBaseMeta, String> databaseO = new HashMap<>();
        databaseO.put(DataBaseMeta.TABLE, DataBaseO.TABLE_METADATA_SQL);
        databaseO.put(DataBaseMeta.COLUMN, DataBaseO.TABLES_COLUMN_META_DATA_SQL);
        databaseO.put(DataBaseMeta.HEALTH, DataBaseMySql.HEALTH_SQL);
        DATABASE_META_MAPPER.put(DataBaseType.O, databaseO);
    }

    /**
     * build sql of query table row count
     *
     * @return table row count sql
     */
    public static String getTableCount() {
        return "select count(1) rowCount from %s.%s";
    }

    /**
     * Return the corresponding metadata execution statement according to the database type
     * and the metadata query type currently to be executed
     *
     * @param databaseType database type
     * @param databaseMeta 数据库元数据
     * @return execute sql
     */
    public static String getMetaSql(DataBaseType databaseType, DataBaseMeta databaseMeta) {
        Assert.isTrue(DATABASE_META_MAPPER.containsKey(databaseType), "Database type mismatch");
        return DATABASE_META_MAPPER.get(databaseType).get(databaseMeta);
    }

    interface DataBaseMySql {
        /**
         * Health check SQL
         */
        String HEALTH_SQL = "select table_name from information_schema.tables  WHERE table_schema=? limit 1";

        /**
         * Table metadata query SQL
         */
        String TABLE_METADATA_SQL = "select info.table_name tableName , info.table_rows tableRows from "
            + " information_schema.tables info where info.table_schema=:databaseSchema "
            + " and info.table_name in ( "
            + " select table_name from information_schema.columns where table_schema=:databaseSchema and  column_key='PRI' )";

        /**
         * column metadata query SQL
         */
        String TABLES_COLUMN_META_DATA_SQL = "select table_name tableName ,column_name columnName,"
            + " ordinal_position ordinalPosition, data_type dataType, column_type columnType,column_key columnKey"
            + " from information_schema.columns"
            + " where table_schema=:databaseSchema and table_name in (:tableNames)";
    }

    interface DataBaseOpenGauss {
        /**
         * Health check SQL
         */
        String HEALTH_SQL = "select table_name from information_schema.tables  WHERE table_schema=? limit 1";

        /**
         * Table metadata query SQL
         */
        String TABLE_METADATA_SQL = "select distinct kcu.table_name tableName , 0 tableRows"
            + " from information_schema.key_column_usage kcu WHERE kcu.constraint_name in ("
            + " select constraint_name from information_schema.table_constraints tc"
            + " where tc.constraint_schema=:databaseSchema and tc.constraint_type='PRIMARY KEY')";

        /**
         * column metadata query SQL
         */
        String TABLES_COLUMN_META_DATA_SQL = "select c.table_name tableName ,c.column_name  columnName, "
            + " c.ordinal_position ordinalPosition, c.data_type dataType , c.data_type columnType,pkc.column_key "
            + " from  information_schema.columns c  left join ( "
            + " select kcu.table_name,kcu.column_name,'PRI' column_key "
            + " from information_schema.key_column_usage kcu " + " WHERE kcu.constraint_name in ("
            + " select constraint_name from information_schema.table_constraints tc"
            + " where tc.constraint_schema=:databaseSchema and tc.constraint_type='PRIMARY KEY' "
            + " ) ) pkc on c.table_name=pkc.table_name and c.column_name=pkc.column_name "
            + " where c.table_schema =:databaseSchema and c.table_name in (:tableNames)";
    }

    interface DataBaseO {
        /**
         * Health check SQL
         */
        String HEALTH_SQL = "";

        /**
         * Table metadata query SQL
         */
        String TABLE_METADATA_SQL = "";

        /**
         * column metadata query SQL
         */
        String TABLES_COLUMN_META_DATA_SQL = "";
    }
}
