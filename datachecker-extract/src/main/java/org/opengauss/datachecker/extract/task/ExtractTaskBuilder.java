package org.opengauss.datachecker.extract.task;

import org.opengauss.datachecker.common.entry.extract.ExtractIncrementTask;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.cache.TableExtractStatusCache;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author wang chao
 * @description 数据抽取任务构建器
 * @date 2022/5/8 19:27
 * @since 11
 **/
@Service
public class ExtractTaskBuilder {
    private static final int EXTRACT_MAX_ROW_COUNT = 100000;
    private static final String TASK_NAME_PREFIX = "TASK_TABLE_";
    private static final String INCREMENT_TASK_NAME_PREFIX = "INCREMENT_TASK_TABLE_";


    /**
     * <pre>
     * 根据元数据缓存信息构建 表数据抽取任务。并初始化数据抽取任务执行状态。
     * 任务构建依赖于元数据缓存信息，以及元数据缓存中加载的当前表记录总数。单个分片任务查询数据总数不超过{@value EXTRACT_MAX_ROW_COUNT}
     * {@code taskCountMap} 用于统计所有待抽取表的分片查询的任务数量
     * {@code tableRows} 为表元数据信息中统计的当前表数据量
     * </pre>
     *
     * @param tableNames 待构建抽取任务表集合
     * @return 任务列表
     */
    public List<ExtractTask> builder(Set<String> tableNames) {
        Assert.isTrue(!CollectionUtils.isEmpty(tableNames), "构建数据抽取任务表不能为空");
        List<ExtractTask> taskList = new ArrayList<>();

        final List<String> tableNameOrderList = tableNames.stream().sorted((tableName1, tableName2) -> {
            TableMetadata metadata1 = MetaDataCache.get(tableName1);
            TableMetadata metadata2 = MetaDataCache.get(tableName2);
            // 排序异常情况处理
            if (Objects.isNull(metadata1) && Objects.isNull(metadata2)) {
                return 0;
            }
            if (Objects.isNull(metadata1)) {
                return -1;
            }
            if (Objects.isNull(metadata2)) {
                return 1;
            }
            return (int) (metadata1.getTableRows() - metadata2.getTableRows());
        }).collect(Collectors.toList());
        // taskCountMap用于统计表分片查询的任务数量
        Map<String, Integer> taskCountMap = new HashMap<>();
        tableNameOrderList.forEach(tableName -> {
            TableMetadata metadata = MetaDataCache.get(tableName);
            if (Objects.nonNull(metadata)) {
                // tableRows为表元数据信息中统计的当前表数据量
                long tableRows = metadata.getTableRows();
                if (tableRows > EXTRACT_MAX_ROW_COUNT) {

                    // 根据表元数据信息构建抽取任务
                    List<ExtractTask> taskEntryList = buildTaskList(metadata);
                    taskCountMap.put(tableName, taskEntryList.size());
                    taskList.addAll(taskEntryList);
                } else {
                    taskList.add(buildTask(metadata));
                    taskCountMap.put(tableName, 1);
                }
            }
        });

        // 初始化数据抽取任务执行状态
        TableExtractStatusCache.init(taskCountMap);
        return taskList;
    }

    private ExtractTask buildTask(TableMetadata metadata) {
        return new ExtractTask().setDivisionsTotalNumber(1)
                .setTableMetadata(metadata)
                .setDivisionsTotalNumber(1)
                .setDivisionsOrdinal(1)
                .setOffset(metadata.getTableRows())
                .setStart(0)
                .setTableName(metadata.getTableName())
                .setTaskName(taskNameBuilder(metadata.getTableName(), 1, 1));
    }


    /**
     * 根据表元数据信息 构建表数据抽取任务。
     * 根据元数据信息中表数据总数估值进行任务分片，单个分片任务查询数据总数不超过 {@value EXTRACT_MAX_ROW_COUNT}
     *
     * @param metadata 元数据信息
     * @return 任务列表
     */
    private List<ExtractTask> buildTaskList(TableMetadata metadata) {
        List<ExtractTask> taskList = new ArrayList<>();
        long tableRows = metadata.getTableRows();
        final int taskCount = calcTaskCount(tableRows);

        IntStream.rangeClosed(1, taskCount).forEach(idx -> {
            long remainingExtractNumber = tableRows - (idx - 1) * EXTRACT_MAX_ROW_COUNT;
            ExtractTask extractTask = buildExtractTask(taskCount, idx, EXTRACT_MAX_ROW_COUNT, remainingExtractNumber);
            extractTask.setDivisionsTotalNumber(taskCount)
                    .setTableMetadata(metadata)
                    .setTableName(metadata.getTableName())
                    .setTaskName(taskNameBuilder(metadata.getTableName(), taskCount, idx));
            taskList.add(extractTask);
        });
        return taskList;
    }

    /**
     * 根据表记录总数，计算分片任务数量
     *
     * @param tableRows 表记录总数
     * @return 分拆任务总数
     */
    private int calcTaskCount(long tableRows) {
        return (int) (tableRows / EXTRACT_MAX_ROW_COUNT);
    }

    /**
     * 任务名称构建
     * <pre>
     * 若任务分拆总数大于1，名称由：前缀信息 {@value TASK_NAME_PREFIX} 、表名称 、表序列 构建
     * 若任务分拆总数为1，即未拆分 ，则根据 前缀信息 {@value TASK_NAME_PREFIX} 、表名称 构建
     * </pre>
     *
     * @param tableName 表名
     * @param taskCount 任务分拆总数
     * @param ordinal   表任务分拆序列
     * @return 任务名称
     */
    private String taskNameBuilder(@NonNull String tableName, int taskCount, int ordinal) {
        if (taskCount > 1) {
            return TASK_NAME_PREFIX.concat(tableName.toUpperCase()).concat("_").concat(String.valueOf(ordinal));
        } else {
            return TASK_NAME_PREFIX.concat(tableName.toUpperCase());
        }
    }

    /**
     * @param taskCount              任务总数
     * @param ordinal                任务序列
     * @param planedExtractNumber    当前任务计划抽取记录总数
     * @param remainingExtractNumber 实际剩余抽取记录总数
     * @return 构建任务对象
     */
    private ExtractTask buildExtractTask(int taskCount, int ordinal, long planedExtractNumber, long remainingExtractNumber) {
        ExtractTask extractTask = new ExtractTask()
                .setDivisionsOrdinal(ordinal)
                .setStart(((ordinal - 1) * planedExtractNumber))
                .setOffset(ordinal == taskCount ? remainingExtractNumber : planedExtractNumber);
        return extractTask;
    }

    /**
     * 增量任务构建
     *
     * @param schema         schema
     * @param sourceDataLogs 增量日志
     * @return 增量任务
     */
    public List<ExtractIncrementTask> buildIncrementTask(String schema, List<SourceDataLog> sourceDataLogs) {
        List<ExtractIncrementTask> incrementTasks = new ArrayList<>();
        sourceDataLogs.forEach(datalog -> {
            incrementTasks.add(new ExtractIncrementTask().setSchema(schema)
                    .setSourceDataLog(datalog)
                    .setTableName(datalog.getTableName())
                    .setTaskName(incrementTaskNameBuilder(datalog.getTableName())));
        });
        return incrementTasks;
    }

    private String incrementTaskNameBuilder(@NonNull String tableName) {
        return INCREMENT_TASK_NAME_PREFIX.concat(tableName.toUpperCase());
    }
}
