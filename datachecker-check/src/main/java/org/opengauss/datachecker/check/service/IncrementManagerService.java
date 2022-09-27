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
import org.opengauss.datachecker.check.modules.check.CheckDiffResult;
import org.opengauss.datachecker.check.modules.check.DataCheckService;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.IdGenerator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

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
    private static final AtomicReference<String> PROCESS_SIGNATURE = new AtomicReference<>();
    @Value("${data.check.data-path}")
    private String path;
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
        dataLogList.addAll(collectLastResults());
        incrementDataLogsChecking(dataLogList);
    }

    private void incrementDataLogsChecking(List<SourceDataLog> dataLogList) {
        String processNo = PROCESS_SIGNATURE.get();
        log.debug("incrementDataLogsChecking {}", dataLogList.size());
        dataLogList.forEach(dataLog -> {
            log.debug("increment data checking {} mode=[{}],{},{}", processNo, CheckMode.INCREMENT,
                dataLog.getTableName(), dataLog.toString());
            // Verify the data according to the table name and Kafka partition
            dataCheckService.incrementCheckTableData(dataLog.getTableName(), processNo, dataLog);
        });
    }

    /**
     * Collect the last verification results and build an incremental verification log
     *
     * @return Analysis of last verification result
     */
    private List<SourceDataLog> collectLastResults() {
        List<SourceDataLog> dataLogList = new ArrayList<>();
        final List<Path> checkResultFileList = FileUtils.loadDirectory(getResultPath());
        List<CheckDiffResult> historyResultList = new ArrayList<>();
        checkResultFileList.forEach(checkResultFile -> {
            try {
                String content = FileUtils.readFileContents(checkResultFile);
                historyResultList.add(JSONObject.parseObject(content, CheckDiffResult.class));
            } catch (CheckingException | JSONException ex) {
                log.error("load check result {} has error", checkResultFile.getFileName());
            }
        });
        parseCheckResult(historyResultList, dataLogList);
        return dataLogList;
    }

    private String getResultPath() {
        String rootPath = path.endsWith(File.separator) ? path : path + File.separator;
        return rootPath + "result" + File.separator;
    }

    private void parseCheckResult(List<CheckDiffResult> historyDataList, List<SourceDataLog> dataLogList) {
        Map<String, SourceDataLog> dataLogMap = new HashMap<>();
        historyDataList.forEach(dataLog -> {
            final String tableName = dataLog.getTable();
            if (dataLogMap.containsKey(tableName)) {
                dataLogMap.get(tableName).getCompositePrimaryValues().addAll(getDiffKeyValues(dataLog));
            } else {
                SourceDataLog sourceDataLog = new SourceDataLog();
                sourceDataLog.setTableName(tableName).setCompositePrimaryValues(getDiffKeyValues(dataLog));
                dataLogMap.put(tableName, sourceDataLog);
            }
        });
        dataLogList.addAll(dataLogMap.values());
    }

    private List<String> getDiffKeyValues(CheckDiffResult dataLog) {
        Set<String> keyValues = new HashSet<>();
        keyValues.addAll(dataLog.getKeyInsertSet());
        keyValues.addAll(dataLog.getKeyUpdateSet());
        keyValues.addAll(dataLog.getKeyDeleteSet());
        return new ArrayList<>(keyValues);
    }
}
