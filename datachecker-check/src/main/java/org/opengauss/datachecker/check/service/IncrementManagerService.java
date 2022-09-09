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
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.modules.check.CheckDiffResult;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.util.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    @Value("${data.check.data-path}")
    private String path;
    @Autowired
    private FeignClientService feignClientService;

    /**
     * Incremental verification log notification
     *
     * @param dataLogList Incremental verification log
     */
    public void notifySourceIncrementDataLogs(List<SourceDataLog> dataLogList) {
        // Collect the last verification results and build an incremental verification log
        dataLogList.addAll(collectLastResults());
        feignClientService.notifyIncrementDataLogs(Endpoint.SOURCE, dataLogList);
        feignClientService.notifyIncrementDataLogs(Endpoint.SINK, dataLogList);
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
