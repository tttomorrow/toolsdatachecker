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

package org.opengauss.datachecker.check.modules.report;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.load.CheckEnvironment;
import org.opengauss.datachecker.check.modules.check.CheckDiffResult;
import org.opengauss.datachecker.check.modules.check.CheckResultConstants;
import org.opengauss.datachecker.common.entry.check.CheckPartition;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.report.CheckFailed;
import org.opengauss.datachecker.common.entry.report.CheckProgress;
import org.opengauss.datachecker.common.entry.report.CheckSuccess;
import org.opengauss.datachecker.common.entry.report.CheckSummary;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.opengauss.datachecker.common.util.TopicUtil;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author ：wangchao
 * @date ：Created in 2023/2/24
 * @since ：11
 */
@Slf4j
@Service
public class CheckResultManagerService {
    private static final String SUMMARY_LOG_NAME = "summary.log";
    private static final String SUCCESS_LOG_NAME = "success.log";
    private static final String FAILED_LOG_NAME = "failed.log";
    private static final String REPAIR_LOG_TEMPLATE = "repair_%s_%s_%s.txt";
    @Resource
    private ProgressService progressService;
    @Resource
    private CheckEnvironment checkEnvironment;
    @Resource
    private FeignClientService feignClient;

    private final Map<CheckPartition, CheckDiffResult> checkResultCache = new ConcurrentHashMap<>();
    private final Map<String, CheckDiffResult> noCheckedCache = new ConcurrentHashMap<>();

    /**
     * Add Merkel verification result
     *
     * @param checkPartition  checkPartition
     * @param checkDiffResult checkDiffResult
     */
    public void addResult(CheckPartition checkPartition, CheckDiffResult checkDiffResult) {
        checkResultCache.put(checkPartition, checkDiffResult);
    }

    /**
     * Summary of verification results
     *
     * @param tableName       tableName
     * @param checkDiffResult checkDiffResult
     */
    public void addNoCheckedResult(String tableName, CheckDiffResult checkDiffResult) {
        noCheckedCache.put(tableName, checkDiffResult);
    }

    /**
     * Summary of verification results
     */
    public void summaryCheckResult() {
        try {
            String logFilePath = getLogRootPath();
            final List<CheckDiffResult> successList = reduceSuccess(logFilePath);
            final List<CheckDiffResult> failedList = reduceFailed(logFilePath);
            reduceFailedRepair(logFilePath, failedList);
            reduceSummary(successList, failedList);
        } catch (Exception exception) {
            log.error("summaryCheckResult ", exception);
        } finally {
            checkResultCache.clear();
            noCheckedCache.clear();
        }
    }

    private void reduceFailedRepair(String logFilePath, List<CheckDiffResult> failedList) {
        failedList.forEach(tableFailed -> {
            final String repairFile = logFilePath + getRepairFileName(tableFailed);
            repairDeleteDiff(repairFile, tableFailed);
            repairInsertDiff(repairFile, tableFailed);
            repairUpdateDiff(repairFile, tableFailed);
        });
    }

    private void repairUpdateDiff(String repairFile, CheckDiffResult tableFailed) {
        final Set<String> updateDiffs = tableFailed.getKeyUpdateSet();
        if (CollectionUtils.isNotEmpty(updateDiffs)) {
            final String schema = tableFailed.getSchema();
            final String table = tableFailed.getTable();
            final List<String> updateRepairs =
                feignClient.buildRepairStatementUpdateDml(Endpoint.SOURCE, schema, table, updateDiffs);
            appendLogFile(repairFile, updateRepairs);
        }
    }

    private void repairInsertDiff(String repairFile, CheckDiffResult tableFailed) {
        final Set<String> insertDiffs = tableFailed.getKeyInsertSet();
        if (CollectionUtils.isNotEmpty(insertDiffs)) {
            final String schema = tableFailed.getSchema();
            final String table = tableFailed.getTable();
            final List<String> insertRepairs =
                feignClient.buildRepairStatementInsertDml(Endpoint.SOURCE, schema, table, insertDiffs);
            appendLogFile(repairFile, insertRepairs);
        }
    }

    private void repairDeleteDiff(String repairFile, CheckDiffResult tableFailed) {
        final Set<String> deleteDiffs = tableFailed.getKeyDeleteSet();
        if (CollectionUtils.isNotEmpty(deleteDiffs)) {
            final String schema = tableFailed.getSchema();
            final String table = tableFailed.getTable();
            final List<String> deleteRepairs =
                feignClient.buildRepairStatementDeleteDml(Endpoint.SOURCE, schema, table, deleteDiffs);
            appendLogFile(repairFile, deleteRepairs);
        }
    }

    private String getRepairFileName(CheckDiffResult tableFailed) {
        final String schema = tableFailed.getSchema();
        final String table = TopicUtil.getTableWithLetter(tableFailed.getTable());
        final int partition = tableFailed.getPartitions();
        return String.format(REPAIR_LOG_TEMPLATE, schema, table, partition);
    }

    private List<CheckDiffResult> reduceFailed(String logFilePath) {
        final List<CheckDiffResult> failedDiffList = filterResultByResult(CheckResultConstants.RESULT_FAILED);
        List<String> failedList = failedDiffList.stream().map(this::translateCheckFailed).map(JsonObjectUtil::prettyFormatMillis)
                                                .collect(Collectors.toList());
        String failedPath = logFilePath + FAILED_LOG_NAME;
        FileUtils.writeAppendFile(failedPath, failedList);
        return failedDiffList;
    }

    private CheckFailed translateCheckFailed(CheckDiffResult result) {
        long cost = calcCheckTaskCost(result);
        return new CheckFailed().setProcess(result.getProcess()).setSchema(result.getSchema())
                                .setTopic(new String[] {result.getTopic()}).setPartition(result.getPartitions())
                                .setTableName(result.getTable()).setCost(cost).setDiffCount(result.getTotalRepair())
                                .setEndTime(result.getEndTime()).setStartTime(result.getStartTime())
                                .setKeyInsertSet(result.getKeyInsertSet()).setKeyDeleteSet(result.getKeyDeleteSet())
                                .setKeyUpdateSet(result.getKeyUpdateSet()).setMessage(result.getMessage());
    }

    private long calcCheckTaskCost(CheckDiffResult result) {
        if (Objects.nonNull(result.getStartTime()) && Objects.nonNull(result.getEndTime())) {
            return Duration.between(result.getStartTime(), result.getEndTime()).toMillis();
        }
        return 0;
    }

    private CheckSuccess translateCheckSuccess(CheckDiffResult result) {
        long cost = calcCheckTaskCost(result);
        return new CheckSuccess().setProcess(result.getProcess()).setSchema(result.getSchema())
                                 .setTopic(new String[] {result.getTopic()}).setPartition(result.getPartitions())
                                 .setTableName(result.getTable()).setCost(cost).setEndTime(result.getEndTime())
                                 .setStartTime(result.getStartTime()).setMessage(result.getMessage());
    }

    private List<CheckDiffResult> reduceSuccess(String logFilePath) {
        final List<CheckDiffResult> successResList = filterResultByResult(CheckResultConstants.RESULT_SUCCESS);
        String successPath = logFilePath + SUCCESS_LOG_NAME;
        List<String> successList = successResList.stream().map(this::translateCheckSuccess).map(JsonObjectUtil::prettyFormatMillis)
                                                 .collect(Collectors.toList());
        FileUtils.writeAppendFile(successPath, successList);
        return successResList;
    }

    private void appendLogFile(String logPath, List<String> resultList) {
        FileUtils.writeAppendFile(logPath, resultList);
    }

    private String getLogRootPath() {
        final String exportCheckPath = checkEnvironment.getExportCheckPath();
        return exportCheckPath + File.separatorChar + "result" + File.separatorChar;
    }

    private void reduceSummary(List<CheckDiffResult> successList, List<CheckDiffResult> failedList) {
        String logFilePath = getLogRootPath();
        int successTableCount = calcTableCount(successList);
        int failedTableCount = calcTableCount(failedList);
        CheckSummary checkSummary = buildCheckSummaryResult(successTableCount, failedTableCount);
        String summaryPath = logFilePath + SUMMARY_LOG_NAME;
        FileUtils.writeFile(summaryPath, JsonObjectUtil.prettyFormatMillis(checkSummary));
    }

    private CheckSummary buildCheckSummaryResult(int successTableCount, int failedTableCount) {
        CheckSummary checkSummary = new CheckSummary();
        final CheckProgress checkProgress = progressService.getCheckProgress();
        checkSummary.setTableCount(successTableCount + failedTableCount);
        checkSummary.setStartTime(checkProgress.getStartTime());
        checkSummary.setEndTime(checkProgress.getEndTime());
        checkSummary.setCost(checkProgress.getCost());
        checkSummary.setSuccessCount(successTableCount);
        checkSummary.setFailedCount(failedTableCount);
        return checkSummary;
    }

    private List<CheckDiffResult> filterResultByResult(String resultType) {
        List<CheckDiffResult> resultList = new LinkedList<>(
            checkResultCache.values().stream().filter(result -> result.getResult().equals(resultType))
                            .collect(Collectors.toList()));
        if (CheckResultConstants.RESULT_FAILED.equals(resultType)) {
            resultList.addAll(noCheckedCache.values());
        }
        return resultList;
    }

    private int calcTableCount(List<CheckDiffResult> resultList) {
        return (int) resultList.stream().map(CheckDiffResult::getTable).distinct().count();
    }
}
