package org.opengauss.datachecker.extract.dao;

import org.opengauss.datachecker.common.entry.enums.CheckBlackWhiteMode;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;

import java.util.List;

public interface MetaDataDAO {
    /**
     * 重置黑白名单
     *
     * @param mode      黑白名单模式{@link CheckBlackWhiteMode}
     * @param tableList 表名称列表
     */
    void resetBlackWhite(CheckBlackWhiteMode mode, List<String> tableList);

    /**
     * 查询表元数据
     *
     * @return 返回表元数据信息
     */
    List<TableMetadata> queryTableMetadata();

    /**
     * 快速查询表元数据 -直接从information_schema获取
     *
     * @return 返回表元数据信息
     */
    List<TableMetadata> queryTableMetadataFast();

    /**
     * 查询表对应列元数据信息
     *
     * @param tableName 表名称
     * @return 列元数据信息
     */
    List<ColumnsMetaData> queryColumnMetadata(String tableName);

    /**
     * 查询表对应列元数据信息
     *
     * @param tableNames 表名称
     * @return 列元数据信息
     */
    List<ColumnsMetaData> queryColumnMetadata(List<String> tableNames);

}
