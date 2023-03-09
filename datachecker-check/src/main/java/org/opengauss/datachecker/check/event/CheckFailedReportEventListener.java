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

package org.opengauss.datachecker.check.event;

import org.opengauss.datachecker.check.load.CheckEnvironment;
import org.opengauss.datachecker.check.modules.check.CheckDiffResult;
import org.opengauss.datachecker.common.entry.report.CheckFailed;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @author ：wangchao
 * @date ：Created in 2023/3/7
 * @since ：11
 */
@Component
public class CheckFailedReportEventListener extends CheckReportEventAdapter
    implements ApplicationListener<CheckFailedReportEvent> {

    private static final String FAILED_LOG_NAME = "failed.log";
    @Resource
    private CheckEnvironment checkEnvironment;

    @Override
    public void onApplicationEvent(CheckFailedReportEvent event) {
        final CheckDiffResult source = (CheckDiffResult) event.getSource();
        FileUtils.writeAppendFile(getFailedPath(), JsonObjectUtil.prettyFormatMillis(translateCheckFailed(source)));
    }

    private String getFailedPath() {
        return getLogRootPath(checkEnvironment.getExportCheckPath()) + FAILED_LOG_NAME;
    }

    private CheckFailed translateCheckFailed(CheckDiffResult result) {
        long cost = calcCheckTaskCost(result.getStartTime(), result.getEndTime());
        return new CheckFailed().setProcess(result.getProcess()).setSchema(result.getSchema())
                                .setTopic(new String[] {result.getTopic()}).setPartition(result.getPartitions())
                                .setTableName(result.getTable()).setCost(cost).setDiffCount(result.getTotalRepair())
                                .setEndTime(result.getEndTime()).setStartTime(result.getStartTime())
                                .setKeyInsertSet(result.getKeyInsertSet()).setKeyDeleteSet(result.getKeyDeleteSet())
                                .setKeyUpdateSet(result.getKeyUpdateSet()).setMessage(result.getMessage());
    }
}
