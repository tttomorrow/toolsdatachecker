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

package org.opengauss.datachecker.extract.debe;

import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.exception.TableNotExistException;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.constants.ExtConstants;

import javax.validation.constraints.NotEmpty;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * DebeziumDataLogs
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/1
 * @since ：11
 */
public class DebeziumDataLogs extends ConcurrentHashMap<String, SourceDataLog> {
    private static final long serialVersionUID = 6477495180190870182L;

    /**
     * get Table log data encapsulation
     *
     * @param tableName tableName
     * @return Table log data encapsulation
     */
    public SourceDataLog getOrDefault(@NotEmpty String tableName) {
        if (containsKey(tableName)) {
            return get(tableName);
        }
        buildDefault(tableName);
        return get(tableName);
    }

    /**
     * Build data log encapsulation object according to table name
     *
     * @param tableName tableName
     */
    private void buildDefault(String tableName) {
        final TableMetadata metadata = MetaDataCache.get(tableName);
        SourceDataLog dataLog = new SourceDataLog();
        if (Objects.nonNull(metadata)) {
            dataLog.setTableName(tableName).setCompositePrimarys(
                metadata.getPrimaryMetas().stream().map(ColumnsMetaData::getColumnName).collect(Collectors.toList()))
                   .setCompositePrimaryValues(new ArrayList<>());
            put(tableName, dataLog);
        } else {
            throw new TableNotExistException(tableName);
        }
    }

    /**
     * Add the {@code tableName} table log to the table log object
     *
     * @param tableName tableName
     * @param valuesMap All field values of the current record
     * @return Add the {@code tableName} table log successfully
     */
    public boolean addDebeziumDataKey(String tableName, Map<String, String> valuesMap) {
        final SourceDataLog dataLog = getOrDefault(tableName);
        if (Objects.nonNull(dataLog)) {
            List<String> primaryValues = new ArrayList<>();
            final List<String> primarys = dataLog.getCompositePrimarys();
            primarys.forEach(primary -> {
                if (valuesMap.containsKey(primary)) {
                    primaryValues.add(valuesMap.get(primary));
                }
            });
            dataLog.getCompositePrimaryValues().add(String.join(ExtConstants.PRIMARY_DELIMITER, primaryValues));
            return true;
        }
        return false;
    }
}
