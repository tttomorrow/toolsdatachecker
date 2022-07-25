package org.opengauss.datachecker.extract.dao;

import org.opengauss.datachecker.common.entry.enums.DataBaseMeta;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;

public class MetaSqlMapper {

    public static String getTableCount() {
        return "select count(1) rowCount from %s.%s";
    }

    interface DataBaseMySql {
        String TABLE_METADATA_SQL = "select table_name tableName , table_rows tableRows from information_schema.tables  WHERE table_schema=?";
        String TABLES_COLUMN_META_DATA_SQL = "select table_name tableName ,column_name  columnName, ordinal_position ordinalPosition, data_type dataType, column_type columnType,column_key columnKey from information_schema.columns where table_schema=:databaseSchema and table_name in (:tableNames)";
    }

    interface DataBaseOpenGauss {
        String TABLE_METADATA_SQL = "select table_name tableName , 0 tableRows from information_schema.tables  WHERE  table_schema=? and TABLE_TYPE='BASE TABLE';";
        String TABLES_COLUMN_META_DATA_SQL = "select c.table_name tableName ,c.column_name  columnName, c.ordinal_position ordinalPosition, c.data_type dataType , c.data_type columnType,pkc.column_key\n" +
                " from  information_schema.columns c \n" +
                " left join (\n" +
                " select kcu.table_name,kcu.column_name,'PRI' column_key\n" +
                " from information_schema.key_column_usage kcu \n" +
                " WHERE kcu.constraint_name in (\n" +
                " select constraint_name from information_schema.table_constraints tc where tc.constraint_schema=:databaseSchema and tc.constraint_type='PRIMARY KEY'\n" +
                " )\n" +
                " ) pkc on c.table_name=pkc.table_name and c.column_name=pkc.column_name\n" +
                " where c.table_schema =:databaseSchema and c.table_name in (:tableNames)";
    }

    interface DataBaseO {
        String TABLE_METADATA_SQL = "";
        String TABLES_COLUMN_META_DATA_SQL = "";
    }

    private static final Map<DataBaseType, Map<DataBaseMeta, String>> DATABASE_META_MAPPER = new HashMap<>();


    static {
        Map<DataBaseMeta, String> dataBaseMySql = new HashMap<>();
        dataBaseMySql.put(DataBaseMeta.TABLE, DataBaseMySql.TABLE_METADATA_SQL);
        dataBaseMySql.put(DataBaseMeta.COLUMN, DataBaseMySql.TABLES_COLUMN_META_DATA_SQL);
        DATABASE_META_MAPPER.put(DataBaseType.MS, dataBaseMySql);

        Map<DataBaseMeta, String> dataBaseOpenGauss = new HashMap<>();
        dataBaseOpenGauss.put(DataBaseMeta.TABLE, DataBaseOpenGauss.TABLE_METADATA_SQL);
        dataBaseOpenGauss.put(DataBaseMeta.COLUMN, DataBaseOpenGauss.TABLES_COLUMN_META_DATA_SQL);
        DATABASE_META_MAPPER.put(DataBaseType.OG, dataBaseOpenGauss);

        Map<DataBaseMeta, String> databaseO = new HashMap<>();
        databaseO.put(DataBaseMeta.TABLE, DataBaseO.TABLE_METADATA_SQL);
        databaseO.put(DataBaseMeta.COLUMN, DataBaseO.TABLES_COLUMN_META_DATA_SQL);
        DATABASE_META_MAPPER.put(DataBaseType.O, databaseO);

    }

    /**
     * 根据数据库类型 以及当前要执行的元数据查询类型 返回对应的元数据执行语句
     *
     * @param dataBaseType 数据库类型
     * @param dataBaseMeta 数据库元数据
     * @return
     */
    public static String getMetaSql(DataBaseType dataBaseType, DataBaseMeta dataBaseMeta) {
        Assert.isTrue(DATABASE_META_MAPPER.containsKey(dataBaseType), "数据库类型不匹配");
        return DATABASE_META_MAPPER.get(dataBaseType).get(dataBaseMeta);
    }
}
