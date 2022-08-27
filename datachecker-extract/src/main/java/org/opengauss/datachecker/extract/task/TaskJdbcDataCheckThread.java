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

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.JsonObjectUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TaskJdbcDataCheckThread
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/27
 * @since ：11
 */
@Slf4j
public class TaskJdbcDataCheckThread extends Thread {
    private final List<RowDataHash> dataRowList;
    private final String taskName;
    private final Endpoint endpoint;
    private final List<String> replateList = new ArrayList<>();

    /**
     * TaskJdbcDataCheckThread
     *
     * @param dataRowList dataRowList
     * @param taskName    taskName
     * @param endpoint    endpoint {@value Endpoint#API_DESCRIPTION}
     */
    public TaskJdbcDataCheckThread(List<RowDataHash> dataRowList, String taskName, Endpoint endpoint) {
        super.setName("DATA_" + taskName);
        this.dataRowList = dataRowList;
        this.taskName = taskName;
        this.endpoint = endpoint;
    }

    /**
     * If this thread was constructed using a separate
     * {@code Runnable} run object, then that
     * {@code Runnable} object's {@code run} method is called;
     * otherwise, this method does nothing and returns.
     * <p>
     * Subclasses of {@code Thread} should override this method.
     *
     * @see #start()
     */
    @Override
    public void run() {
        String path = "." + File.separator + "data";
        String fileName = path + File.separator + taskName + "_" + endpoint.getDescription() + ".json";
        FileUtils.createDirectories(path);
        FileUtils.deleteFile(fileName);
        FileUtils.writeAppendFile(fileName, JsonObjectUtil.format(dataRowList));
        Map<String, RowDataHash> dataMap = new HashMap<>(InitialCapacity.CAPACITY_16);
        dataRowList.forEach(row -> {
            if (dataMap.containsKey(row.getPrimaryKey())) {
                replateList.add(row.getPrimaryKey());
            } else {
                dataMap.put(row.getPrimaryKey(), row);
            }
        });
        log.debug("dataRowList:{}", dataRowList.size());
        log.debug("dataMap:{}", dataMap.size());
        log.debug("replateList:{}", replateList);
    }
}
