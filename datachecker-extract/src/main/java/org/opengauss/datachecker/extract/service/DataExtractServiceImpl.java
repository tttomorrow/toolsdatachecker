package org.opengauss.datachecker.extract.service;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.constant.Constants;
import org.opengauss.datachecker.common.entry.enums.DML;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.*;
import org.opengauss.datachecker.common.exception.ProcessMultipleException;
import org.opengauss.datachecker.common.exception.TableNotExistException;
import org.opengauss.datachecker.common.exception.TaskNotFoundException;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.cache.TableExtractStatusCache;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.kafka.KafkaAdminService;
import org.opengauss.datachecker.extract.kafka.KafkaCommonService;
import org.opengauss.datachecker.extract.task.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.DependsOn;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.validation.constraints.NotEmpty;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
@Service
@DependsOn("extractThreadExecutor")
public class DataExtractServiceImpl implements DataExtractService {


    /**
     * 执行数据抽取任务的线程 最大休眠次数
     */
    private static final int MAX_SLEEP_COUNT = 30;
    /**
     * 执行数据抽取任务的线程 每次休眠时间，单位毫秒
     */
    private static final int MAX_SLEEP_MILLIS_TIME = 2000;
    private static final String PROCESS_NO_RESET = "0";

    /**
     * 服务启动后，会对{code atomicProcessNo}属性进行初始化，
     * <p>
     * 用户启动校验流程，会对{code atomicProcessNo}属性进行校验和设置
     */
    private final AtomicReference<String> atomicProcessNo = new AtomicReference<>(PROCESS_NO_RESET);

    private final AtomicReference<List<ExtractTask>> taskReference = new AtomicReference<>();
    private final AtomicReference<List<ExtractIncrementTask>> incrementTaskReference = new AtomicReference<>();

    @Autowired
    @Qualifier("extractThreadExecutor")
    private ThreadPoolTaskExecutor extractThreadExecutor;

    @Autowired
    private ExtractTaskBuilder extractTaskBuilder;

    @Autowired
    private ExtractThreadSupport extractThreadSupport;

    @Autowired
    private IncrementExtractThreadSupport incrementExtractThreadSupport;

    @Autowired
    private CheckingFeignClient checkingFeignClient;

    @Autowired
    private ExtractProperties extractProperties;

    @Autowired
    private KafkaCommonService kafkaCommonService;

    @Autowired
    private KafkaAdminService kafkaAdminService;

    @Autowired
    private DataManipulationService dataManipulationService;

    /**
     * 数据抽取服务
     * <p>
     * 校验服务通过下发数据抽取流程请求，抽取服务对进程号进行校验，防止同一时间重复发起启动命令
     * <p>
     * 根据元数据缓存信息，构建数据抽取任务，保存当前任务信息到{@code taskReference}中，等待校验服务发起任务执行指令。
     * 上报任务列表到校验服务。
     *
     * @param processNo 执行进程编号
     * @throws ProcessMultipleException 当前实例正在执行数据抽取服务，不能重新开启新的校验。
     */
    @Override
    public List<ExtractTask> buildExtractTaskAllTables(String processNo) throws ProcessMultipleException {
        // 调用端点不是源端，则直接返回空
        if (!Objects.equals(extractProperties.getEndpoint(), Endpoint.SOURCE)) {
            log.info("The current endpoint is not the source endpoint, and the task cannot be built");
            return Collections.EMPTY_LIST;
        }
        if (atomicProcessNo.compareAndSet(PROCESS_NO_RESET, processNo)) {
            Set<String> tableNames = MetaDataCache.getAllKeys();
            List<ExtractTask> taskList = extractTaskBuilder.builder(tableNames);
            if (CollectionUtils.isEmpty(taskList)) {
                return taskList;
            }
            taskReference.set(taskList);
            log.info("build extract task process={} count={}", processNo, taskList.size());
            atomicProcessNo.set(processNo);

            List<String> taskNameList = taskList.stream()
                    .map(ExtractTask::getTaskName)
                    .map(String::toUpperCase)
                    .collect(Collectors.toList());
            initTableExtractStatus(new ArrayList<>(tableNames));
            return taskList;
        } else {
            log.error("process={} is running extract task , {} please wait ... ", atomicProcessNo.get(), processNo);
            throw new ProcessMultipleException("process {" + atomicProcessNo.get() + "} is running extract task");
        }
    }

    /**
     * 宿端任务配置
     *
     * @param processNo 执行进程编号
     * @param taskList  任务列表
     * @throws ProcessMultipleException 前实例正在执行数据抽取服务，不能重新开启新的校验。
     */
    @Override
    public void buildExtractTaskAllTables(String processNo, @NonNull List<ExtractTask> taskList) throws ProcessMultipleException {
        if (!Objects.equals(extractProperties.getEndpoint(), Endpoint.SINK)) {
            return;
        }
        // 校验源端构建的任务列表 在宿端是否存在 ，将不存在任务列表过滤
        final Set<String> tableNames = MetaDataCache.getAllKeys();
        if (atomicProcessNo.compareAndSet(PROCESS_NO_RESET, processNo)) {
            if (CollectionUtils.isEmpty(taskList) || CollectionUtils.isEmpty(tableNames)) {
                return;
            }
            final List<ExtractTask> extractTasks = taskList.stream()
                    .filter(task -> tableNames.contains(task.getTableName()))
                    .collect(Collectors.toList());
            taskReference.set(extractTasks);
            log.info("build extract task process={} count={}", processNo, extractTasks.size());
            atomicProcessNo.set(processNo);

            // taskCountMap用于统计表分片查询的任务数量
            Map<String, Integer> taskCountMap = new HashMap<>(Constants.InitialCapacity.MAP);
            taskList.forEach(task -> {
                if (!taskCountMap.containsKey(task.getTableName())) {
                    taskCountMap.put(task.getTableName(), task.getDivisionsTotalNumber());
                }
            });
            // 初始化数据抽取任务执行状态
            TableExtractStatusCache.init(taskCountMap);

            final List<String> filterTaskTables = taskList.stream()
                    .filter(task -> !tableNames.contains(task.getTableName()))
                    .map(ExtractTask::getTableName)
                    .distinct()
                    .collect(Collectors.toList());
            if (!CollectionUtils.isEmpty(filterTaskTables)) {
                log.info("process={} ,source endpoint database have some tables ,not in the sink tables[{}]",
                        processNo, filterTaskTables);
            }
        } else {
            log.error("process={} is running extract task , {} please wait ... ", atomicProcessNo.get(), processNo);
            throw new ProcessMultipleException("process {" + atomicProcessNo.get() + "} is running extract task");
        }
    }

    /**
     * 清理当前构建任务
     */
    @Override
    public void cleanBuildedTask() {
        if (Objects.nonNull(taskReference.getAcquire())) {
            taskReference.getAcquire().clear();
        }
        if (Objects.nonNull(incrementTaskReference.getAcquire())) {
            incrementTaskReference.getAcquire().clear();
        }
        TableExtractStatusCache.removeAll();
        atomicProcessNo.set(PROCESS_NO_RESET);
        log.info("clear the current build task cache!");
        log.info("clear extraction service status flag!");
    }

    /**
     * 查询当前执行流程下，指定表的数据抽取相关信息
     *
     * @param tableName 表名称
     * @return 表的数据抽取相关信息
     */
    @Override
    public ExtractTask queryTableInfo(String tableName) {
        List<ExtractTask> taskList = taskReference.get();
        Optional<ExtractTask> taskEntry = Optional.empty();
        if (!CollectionUtils.isEmpty(taskList)) {
            for (ExtractTask task : taskList) {
                if (Objects.equals(task.getTableName(), tableName)) {
                    taskEntry = Optional.of(task);
                    break;
                }
            }
        }
        if (taskEntry.isEmpty()) {
            throw new TaskNotFoundException(tableName);
        }
        return taskEntry.get();
    }

    /**
     * 执行指定进程编号的数据抽取任务。
     * <p>
     * 执行抽取任务，对当前进程编号进行校验，并对抽取任务进行校验。
     * 对于抽取任务的校验，采用轮询方式，进行多次校验。
     * 因为源端和宿端的抽取执行逻辑是异步且属于不同的Java进程。为确保不同进程之间流程数据状态一致，采用轮询方式多次进行确认。
     * 若多次确认还不能获取任务数据{@code taskReference}中数据为空，则抛出异常{@link org.opengauss.datachecker.common.exception.TaskNotFoundException}
     *
     * @param processNo 执行进程编号
     * @throws TaskNotFoundException 任务数据为空，则抛出异常 TaskNotFoundException
     */
    @Async
    @Override
    public void execExtractTaskAllTables(String processNo) throws TaskNotFoundException {
        if (Objects.equals(atomicProcessNo.get(), processNo)) {
            int sleepCount = 0;
            while (CollectionUtils.isEmpty(taskReference.get())) {
                ThreadUtil.sleep(MAX_SLEEP_MILLIS_TIME);
                if (sleepCount++ > MAX_SLEEP_COUNT) {
                    log.info("endpoint [{}] and process[{}}] task is empty!", extractProperties.getEndpoint().getDescription(), processNo);
                    break;
                }
            }
            List<ExtractTask> taskList = taskReference.get();
            if (CollectionUtils.isEmpty(taskList)) {
                return;
            }
            taskList.forEach(task -> {
                log.info("执行数据抽取任务：{}", task);
                ThreadUtil.sleep(100);
                Topic topic = kafkaCommonService.getTopicInfo(processNo, task.getTableName(), task.getDivisionsTotalNumber());
                kafkaAdminService.createTopic(topic.getTopicName(), topic.getPartitions());
                extractThreadExecutor.submit(new ExtractTaskThread(task, topic, extractThreadSupport));
            });
        }
    }

    /**
     * 生成修复报告的DML语句
     *
     * @param tableName 表名
     * @param dml       dml 类型
     * @param diffSet   待生成主键集合
     * @return DML语句
     */
    @Override
    public List<String> buildRepairDml(String schema, @NotEmpty String tableName, @NonNull DML dml, @NotEmpty Set<String> diffSet) {
        if (CollectionUtils.isEmpty(diffSet)) {
            return new ArrayList<>();
        }
        List<String> resultList = new ArrayList<>();
        final TableMetadata metadata = MetaDataCache.get(tableName);
        final List<ColumnsMetaData> primaryMetas = metadata.getPrimaryMetas();

        if (Objects.equals(dml, DML.DELETE)) {
            resultList.addAll(dataManipulationService.buildDelete(schema, tableName, diffSet, primaryMetas));
        } else if (Objects.equals(dml, DML.INSERT)) {
            resultList.addAll(dataManipulationService.buildInsert(schema, tableName, diffSet, metadata));
        } else if (Objects.equals(dml, DML.REPLACE)) {
            resultList.addAll(dataManipulationService.buildReplace(schema, tableName, diffSet, metadata));
        }
        return resultList;
    }

    /**
     * 查询表数据
     *
     * @param tableName     表名称
     * @param compositeKeys 复核主键集合
     * @return 主键对应表数据
     */
    @Override
    public List<Map<String, String>> queryTableColumnValues(String tableName, List<String> compositeKeys) {
        final TableMetadata metadata = MetaDataCache.get(tableName);
        if (Objects.isNull(metadata)) {
            throw new TableNotExistException(tableName);
        }
        return dataManipulationService.queryColumnValues(tableName, new ArrayList<>(compositeKeys), metadata);
    }

    /**
     * 根据数据变更日志 构建增量抽取任务
     *
     * @param sourceDataLogs 数据变更日志
     */
    @Override
    public void buildExtractIncrementTaskByLogs(List<SourceDataLog> sourceDataLogs) {
        final String schema = extractProperties.getSchema();
        List<ExtractIncrementTask> taskList = extractTaskBuilder.buildIncrementTask(schema, sourceDataLogs);
        log.info("构建增量抽取任务完成：{}", taskList.size());
        if (CollectionUtils.isEmpty(taskList)) {
            return;
        }
        incrementTaskReference.set(taskList);

        List<String> tableNameList = sourceDataLogs.stream()
                .map(SourceDataLog::getTableName)
                .collect(Collectors.toList());
        Map<String, Integer> taskCount = new HashMap<>(Constants.InitialCapacity.MAP);
        createTaskCountMapping(tableNameList, taskCount);
        TableExtractStatusCache.init(taskCount);
        initTableExtractStatus(tableNameList);
    }

    private void createTaskCountMapping(List<String> tableNameList, Map<String, Integer> taskCount) {
        tableNameList.forEach(table -> {
            taskCount.put(table, 1);
        });
    }

    /**
     * 执行增量校验数据抽取
     */
    @Override
    public void execExtractIncrementTaskByLogs() {

        List<ExtractIncrementTask> taskList = incrementTaskReference.get();
        if (CollectionUtils.isEmpty(taskList)) {
            log.info("endpoint [{}] task is empty!", extractProperties.getEndpoint().getDescription());
            return;
        }
        taskList.forEach(task -> {
            log.info("执行数据抽取任务：{}", task);
            ThreadUtil.sleep(100);
            Topic topic = kafkaCommonService.getIncrementTopicInfo(task.getTableName());
            kafkaAdminService.createTopic(topic.getTopicName(), topic.getPartitions());
            extractThreadExecutor.submit(new IncrementExtractTaskThread(task, topic, incrementExtractThreadSupport));
        });
    }

    /**
     * 查询当前表结构元数据信息，并进行Hash
     *
     * @param tableName 表名称
     * @return 表结构Hash
     */
    @Override
    public TableMetadataHash queryTableMetadataHash(String tableName) {
        return dataManipulationService.queryTableMetadataHash(tableName);
    }

    /**
     * 查询表指定PK列表数据，并进行Hash 用于二次校验数据查询
     *
     * @param dataLog 数据日志
     * @return rowdata hash
     */
    @Override
    public List<RowDataHash> querySecondaryCheckRowData(SourceDataLog dataLog) {
        final String tableName = dataLog.getTableName();
        final List<String> compositeKeys = dataLog.getCompositePrimaryValues();

        final TableMetadata metadata = MetaDataCache.get(tableName);
        if (Objects.isNull(metadata)) {
            throw new TableNotExistException(tableName);
        }
        List<Map<String, String>> dataRowList = dataManipulationService.queryColumnValues(tableName, compositeKeys, metadata);
        RowDataHashHandler handler = new RowDataHashHandler();
        return handler.handlerQueryResult(metadata, dataRowList);
    }

    @Override
    public String queryDatabaseSchema() {
        return extractProperties.getSchema();
    }


    private void initTableExtractStatus(List<String> tableNameList) {
        if (Objects.equals(extractProperties.getEndpoint(), Endpoint.SOURCE)) {
            checkingFeignClient.initTableExtractStatus(new ArrayList<>(tableNameList));
            log.info("通知校验服务初始化增量抽取任务状态：{}", tableNameList);
        }
    }
}
