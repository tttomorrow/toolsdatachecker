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

package org.opengauss.datachecker.extract.task;

import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.util.HashHandler;
import org.opengauss.datachecker.extract.util.MetaDataUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/17
 * @since ：11
 */
public class RowDataHashHandler {
    /**
     * According to the column order in the table metadata information {@code tableMetadata},
     * the queried data results are spliced, and the hash calculation of the spliced result rows is performed
     *
     * @param tableMetadata Table metadata information
     * @param dataRowList   Query data set
     * @return Returns the hash calculation result of extracted data
     */
    public List<RowDataHash> handlerQueryResult(TableMetadata tableMetadata, List<Map<String, String>> dataRowList) {
        List<RowDataHash> recordHashList = Collections.synchronizedList(new ArrayList<>());
        HashHandler hashHandler = new HashHandler();
        List<String> columns = MetaDataUtil.getTableColumns(tableMetadata);
        List<String> primarys = MetaDataUtil.getTablePrimaryColumns(tableMetadata);
        dataRowList.forEach(rowColumnsValueMap -> {
            long rowHash = hashHandler.xx3Hash(rowColumnsValueMap, columns);
            String primaryValue = hashHandler.value(rowColumnsValueMap, primarys);
            long primaryHash = hashHandler.xx3Hash(rowColumnsValueMap, primarys);
            RowDataHash hashData = new RowDataHash();
            hashData.setPrimaryKey(primaryValue).setPrimaryKeyHash(primaryHash).setRowHash(rowHash);
            recordHashList.add(hashData);
        });
        return recordHashList;
    }
}
