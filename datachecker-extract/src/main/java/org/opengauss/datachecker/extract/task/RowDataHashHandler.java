package org.opengauss.datachecker.extract.task;

import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.util.HashHandler;
import org.opengauss.datachecker.extract.util.MetaDataUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/17
 * @since ：11
 */
public class RowDataHashHandler {

    /**
     * 根据表元数据信息{@code tableMetadata}中列顺序，对查询出的数据结果进行拼接，并对拼接后的结果行哈希计算
     *
     * @param tableMetadata 表元数据信息
     * @param dataRowList   查询数据集合
     * @return 返回抽取数据的哈希计算结果
     */
    public List<RowDataHash> handlerQueryResult(TableMetadata tableMetadata, List<Map<String, String>> dataRowList) {

        List<RowDataHash> recordHashList = new ArrayList<>();
        HashHandler hashHandler = new HashHandler();
        List<String> columns = MetaDataUtil.getTableColumns(tableMetadata);
        List<String> primarys = MetaDataUtil.getTablePrimaryColumns(tableMetadata);
        dataRowList.forEach(rowColumnsValueMap -> {
            long rowHash = hashHandler.xx3Hash(rowColumnsValueMap, columns);

            String primaryValue = hashHandler.value(rowColumnsValueMap, primarys);
            long primaryHash = hashHandler.xx3Hash(rowColumnsValueMap, primarys);
            RowDataHash hashData = new RowDataHash()
                    .setPrimaryKey(primaryValue)
                    .setPrimaryKeyHash(primaryHash)
                    .setRowHash(rowHash);
            recordHashList.add(hashData);
        });
        return recordHashList;
    }
}
