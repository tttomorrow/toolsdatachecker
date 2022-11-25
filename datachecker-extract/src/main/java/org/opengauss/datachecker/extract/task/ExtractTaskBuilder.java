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

import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.opengauss.datachecker.common.entry.extract.ExtractIncrementTask;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.TaskUtil;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.cache.TableExtractStatusCache;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Data extraction task builder
 *
 * @author wang chao
 * @date 2022/5/8 19:27
 * @since 11
 **/
@Service
public class ExtractTaskBuilder {
    private static final int EXTRACT_MAX_ROW_COUNT = TaskUtil.EXTRACT_MAX_ROW_COUNT;
    private static final String TASK_NAME_PREFIX = "TASK_TABLE_";
    private static final String INCREMENT_TASK_NAME_PREFIX = "INCREMENT_TASK_TABLE_";

    /**
     * <pre>
     * Construct the table data extraction task according to the metadata cache information.
     * And initialize the execution state of the data extraction task.
     * Task construction depends on metadata cache information and the total number of current table records
     * loaded in the metadata cache.
     * The total number of query data of a single fragment task does not exceed {@value EXTRACT_MAX_ROW_COUNT}
     * {@code taskCountMap} It is used to count the number of tasks of fragment query of all tables to be extracted
     * {@code tableRows} Is the current table data amount counted in the table metadata information
     * </pre>
     *
     * @param tableNames Extraction task table set to be built
     * @return task list
     */
    public List<ExtractTask> builder(Set<String> tableNames) {
        Assert.isTrue(!CollectionUtils.isEmpty(tableNames), "Build data extraction task table cannot be empty");
        List<ExtractTask> taskList = new ArrayList<>();

        final List<String> tableNameOrderList =
            tableNames.stream().filter(MetaDataCache::containsKey).sorted((tableName1, tableName2) -> {
                TableMetadata metadata1 = MetaDataCache.get(tableName1);
                TableMetadata metadata2 = MetaDataCache.get(tableName2);
                return (int) (metadata1.getTableRows() - metadata2.getTableRows());
            }).collect(Collectors.toList());

        // taskCountMap is used to count the number of tasks in table fragment query
        Map<String, Integer> taskCountMap = new HashMap<>(InitialCapacity.CAPACITY_16);
        tableNameOrderList.forEach(tableName -> {
            TableMetadata metadata = MetaDataCache.get(tableName);
            if (Objects.nonNull(metadata)) {
                // tableRows is the current table data amount counted in the table metadata information
                long tableRows = metadata.getTableRows();
                if (tableRows > EXTRACT_MAX_ROW_COUNT) {

                    // Construct extraction tasks based on table metadata information
                    List<ExtractTask> taskEntryList = buildTaskList(metadata);
                    taskCountMap.put(tableName, taskEntryList.size());
                    taskList.addAll(taskEntryList);
                } else {
                    taskList.add(buildTask(metadata));
                    taskCountMap.put(tableName, 1);
                }
            }
        });

        // Initialization data extraction task execution status
        TableExtractStatusCache.init(taskCountMap);
        return taskList;
    }

    private ExtractTask buildTask(TableMetadata metadata) {
        return new ExtractTask().setTableMetadata(metadata).setOffset(metadata.getTableRows())
                                .setTableName(metadata.getTableName())
                                .setTaskName(taskNameBuilder(metadata.getTableName(), 1, 1));
    }

    /**
     * <pre>
     * The table data extraction task is constructed according to the table metadata information.
     * Task segmentation is carried out according to the estimation
     * of the total amount of table data in the metadata information.
     * The total amount of query data of a single segmented task does not exceed {@value EXTRACT_MAX_ROW_COUNT}
     * </pre>
     *
     * @param metadata metadata information
     * @return task list
     */
    private List<ExtractTask> buildTaskList(TableMetadata metadata) {
        List<ExtractTask> taskList = new ArrayList<>();
        long tableRows = metadata.getTableRows();
        final int taskCount = TaskUtil.calcTaskCount(tableRows);

        IntStream.rangeClosed(1, taskCount).forEach(idx -> {
            long remainingExtractNumber = tableRows - (idx - 1) * EXTRACT_MAX_ROW_COUNT;
            ExtractTask extractTask = buildExtractTask(taskCount, idx, EXTRACT_MAX_ROW_COUNT, remainingExtractNumber);
            extractTask.setDivisionsTotalNumber(taskCount).setTableMetadata(metadata)
                       .setTableName(metadata.getTableName())
                       .setTaskName(taskNameBuilder(metadata.getTableName(), taskCount, idx));
            taskList.add(extractTask);
        });
        return taskList;
    }

    /**
     * Task name build
     * <pre>
     * If the total number of task partitions is greater than 1,
     * the name is constructed by: prefix information {@value TASK_NAME_PREFIX}, table name, and table sequence
     * If the total number of task splits is 1, that is, it is not split,
     * it is built according to the prefix information {@value TASK_NAME_PREFIX}, table name
     * </pre>
     *
     * @param tableName tableName
     * @param taskCount Total number of task splits
     * @param ordinal   Table task split sequence
     * @return task name
     */
    private String taskNameBuilder(@NonNull String tableName, int taskCount, int ordinal) {
        if (taskCount > 1) {
            return TASK_NAME_PREFIX.concat(tableName.toUpperCase(Locale.ROOT)).concat("_")
                                   .concat(String.valueOf(ordinal));
        } else {
            return TASK_NAME_PREFIX.concat(tableName.toUpperCase(Locale.ROOT));
        }
    }

    /**
     * @param taskCount       Total number of task splits
     * @param ordinal         Table task split sequence
     * @param planedNumber    Total number of current task plan extraction records
     * @param remainingNumber Total number of actual remaining extraction records
     * @return Build task object
     */
    private ExtractTask buildExtractTask(int taskCount, int ordinal, long planedNumber, long remainingNumber) {
        long start = (ordinal - 1) * planedNumber;
        long offset = ordinal == taskCount ? remainingNumber : planedNumber;
        return new ExtractTask().setDivisionsOrdinal(ordinal).setStart(start).setOffset(offset);
    }

    /**
     * Incremental task construction
     *
     * @param schema         schema
     * @param sourceDataLogs Incremental log
     * @return Incremental task
     */
    public List<ExtractIncrementTask> buildIncrementTask(String schema, List<SourceDataLog> sourceDataLogs) {
        List<ExtractIncrementTask> incrementTasks = new ArrayList<>();
        sourceDataLogs.forEach(datalog -> {
            incrementTasks.add(new ExtractIncrementTask().setSchema(schema).setSourceDataLog(datalog)
                                                         .setTableName(datalog.getTableName()).setTaskName(
                    incrementTaskNameBuilder(datalog.getTableName())));
        });
        return incrementTasks;
    }

    private String incrementTaskNameBuilder(@NonNull String tableName) {
        return INCREMENT_TASK_NAME_PREFIX.concat(tableName.toUpperCase(Locale.ROOT));
    }
}
