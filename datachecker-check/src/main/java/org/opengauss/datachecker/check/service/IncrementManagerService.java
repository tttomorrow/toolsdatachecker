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

package org.opengauss.datachecker.check.service;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.check.modules.check.CheckDiffResult;
import org.opengauss.datachecker.check.modules.check.DataCheckService;
import org.opengauss.datachecker.check.modules.check.ExportCheckResult;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.exception.LargeDataDiffException;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.IdGenerator;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * IncrementManagerService
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/14
 * @since ：11
 */
@Slf4j
@Service
public class IncrementManagerService {
    private static final int MAX_CHECK_DIFF_SIZE = 5000;
    private static final AtomicReference<String> PROCESS_SIGNATURE = new AtomicReference<>();
    @Resource
    private DataCheckService dataCheckService;

    /**
     * Incremental verification log notification
     *
     * @param dataLogList Incremental verification log
     */
    public void notifySourceIncrementDataLogs(List<SourceDataLog> dataLogList) {
        if (CollectionUtils.isEmpty(dataLogList)) {
            return;
        }
        PROCESS_SIGNATURE.set(IdGenerator.nextId36());
        // Collect the last verification results and build an incremental verification log
        mergeDataLogList(dataLogList, collectLastResults());
        ExportCheckResult.backCheckResultDirectory();
        incrementDataLogsChecking(dataLogList);
    }

    private void mergeDataLogList(List<SourceDataLog> dataLogList, Map<String, SourceDataLog> collectLastResults) {
        final Map<String, SourceDataLog> dataLogMap =
            dataLogList.stream().collect(Collectors.toMap(SourceDataLog::getTableName, Function.identity()));
        collectLastResults.forEach((tableName, lastLog) -> {
            final int cnt = lastLog.getCompositePrimaryValues().size();
            if (cnt > MAX_CHECK_DIFF_SIZE) {
                dataLogList.remove(dataLogMap.get(tableName));
                log.error("table={} begin offset ={} data has large different rows,please synchronized repeat! ",
                    tableName, lastLog.getBeginOffset());
                throw new LargeDataDiffException("table=" + tableName + " data has large different (" + cnt + ") rows");
            } else if (dataLogMap.containsKey(tableName)) {
                final List<String> values = dataLogMap.get(tableName).getCompositePrimaryValues();
                final long beginOffset = Math.min(dataLogMap.get(tableName).getBeginOffset(), lastLog.getBeginOffset());
                final Set<String> margeValueSet = new HashSet<>();
                margeValueSet.addAll(values);
                margeValueSet.addAll(lastLog.getCompositePrimaryValues());
                dataLogMap.get(tableName).getCompositePrimaryValues().clear();
                dataLogMap.get(tableName).setBeginOffset(beginOffset);
                dataLogMap.get(tableName).getCompositePrimaryValues().addAll(margeValueSet);
            } else {
                dataLogList.add(lastLog);
            }
        });
    }

    private void incrementDataLogsChecking(List<SourceDataLog> dataLogList) {
        String processNo = PROCESS_SIGNATURE.get();
        log.debug("incrementDataLogsChecking datalog size= {}", dataLogList.size());
        dataLogList.forEach(dataLog -> {
            log.debug("increment process=[{}] , tableName=[{}], begin offset =[{}], diffSize=[{}]", processNo,
                dataLog.getTableName(), dataLog.getBeginOffset(), dataLog.getCompositePrimaryValues().size());
            // Verify the data according to the table name and Kafka partition
            dataCheckService.incrementCheckTableData(dataLog.getTableName(), processNo, dataLog);
        });
    }

    /**
     * Collect the last verification results and build an incremental verification log
     *
     * @return Analysis of last verification result
     */
    private Map<String, SourceDataLog> collectLastResults() {
        final List<Path> checkResultFileList = FileUtils.loadDirectory(ExportCheckResult.getResultPath());
        if (CollectionUtils.isEmpty(checkResultFileList)) {
            return new HashMap<>();
        }
        List<CheckDiffResult> historyResultList = new ArrayList<>();
        checkResultFileList.forEach(checkResultFile -> {
            try {
                String content = FileUtils.readFileContents(checkResultFile);
                historyResultList.add(JSONObject.parseObject(content, CheckDiffResult.class));
            } catch (CheckingException | JSONException ex) {
                log.error("load check result {} has error", checkResultFile.getFileName());
            }
        });

        return parseCheckResult(historyResultList);
    }

    private Map<String, SourceDataLog> parseCheckResult(List<CheckDiffResult> historyDataList) {
        Map<String, SourceDataLog> dataLogMap = new HashMap<>();
        historyDataList.forEach(dataLog -> {
            if (StringUtils.equals(dataLog.getResult(), CheckDiffResult.FAILED_RESULT)) {
                final Set<String> diffKeyValues = getDiffKeyValues(dataLog);
                final String tableName = dataLog.getTable();
                if (dataLogMap.containsKey(tableName)) {
                    final SourceDataLog dataLogMarge = dataLogMap.get(tableName);
                    final long beginOffset = Math.min(dataLogMarge.getBeginOffset(), dataLog.getBeginOffset());
                    final List<String> values = dataLogMarge.getCompositePrimaryValues();
                    diffKeyValues.addAll(values);
                    dataLogMarge.getCompositePrimaryValues().clear();
                    dataLogMarge.getCompositePrimaryValues().addAll(diffKeyValues);
                    dataLogMarge.setBeginOffset(beginOffset);
                } else {
                    SourceDataLog sourceDataLog = new SourceDataLog();
                    sourceDataLog.setTableName(tableName).setBeginOffset(dataLog.getBeginOffset())
                                 .setCompositePrimaryValues(new ArrayList<>(diffKeyValues));
                    dataLogMap.put(tableName, sourceDataLog);
                }
            }
        });
        return dataLogMap;
    }

    private Set<String> getDiffKeyValues(CheckDiffResult dataLog) {
        Set<String> keyValues = new HashSet<>();
        keyValues.addAll(dataLog.getKeyInsertSet());
        keyValues.addAll(dataLog.getKeyUpdateSet());
        keyValues.addAll(dataLog.getKeyDeleteSet());
        return keyValues;
    }
}
