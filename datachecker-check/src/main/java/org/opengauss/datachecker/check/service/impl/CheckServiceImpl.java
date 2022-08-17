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

package org.opengauss.datachecker.check.service.impl;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.check.annotation.Statistical;
import org.opengauss.datachecker.check.cache.TableStatusRegister;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.config.DataCheckProperties;
import org.opengauss.datachecker.check.modules.check.DataCheckService;
import org.opengauss.datachecker.check.modules.check.ExportCheckResult;
import org.opengauss.datachecker.check.service.CheckService;
import org.opengauss.datachecker.check.service.EndpointMetaDataManager;
import org.opengauss.datachecker.common.entry.check.IncrementCheckConfig;
import org.opengauss.datachecker.common.entry.check.Pair;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.exception.CheckingPollingException;
import org.opengauss.datachecker.common.exception.CommonException;
import org.opengauss.datachecker.common.util.IdGenerator;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
@Slf4j
@Service(value = "checkService")
public class CheckServiceImpl implements CheckService {
    /**
     * Verification task start flag
     * <p>
     * Whether full or incremental verification, only one can be performed at a time.
     * Only after the local full or incremental verification is completed, that is,
     * {@code started}=false, can the next one be executed.
     * Otherwise, exit directly and wait until the current verification process is completed,
     * and then exit automatically.
     * <p>
     * The method of forcibly exiting the current verification process is not provided for the time being.
     */
    private static final AtomicBoolean STARTED = new AtomicBoolean(false);
    private static final AtomicBoolean CHECKING = new AtomicBoolean(true);

    /**
     * Process signature
     */
    private static final AtomicReference<String> PROCESS_SIGNATURE = new AtomicReference<>();

    /**
     * Verify Mode
     */
    private static final AtomicReference<CheckMode> CHECK_MODE_REF = new AtomicReference<>();

    /**
     * Verify polling thread name
     */
    private static final String SELF_CHECK_POLL_THREAD_NAME = "check-polling-thread";

    private static final String START_MESSAGE = "the execution time of %s process is %s";

    @Autowired
    private FeignClientService feignClientService;

    @Autowired
    private TableStatusRegister tableStatusRegister;

    @Resource
    private DataCheckService dataCheckService;

    @Autowired
    private DataCheckProperties properties;

    @Autowired
    private EndpointMetaDataManager endpointMetaDataManager;

    @Value("${data.check.auto-clean-environment}")
    private boolean isAutoCleanEnvironment = true;

    @Value("${data.check.check-with-sync-extracting}")
    private boolean isCheckWithSyncExtracting = true;

    /**
     * Initialize the verification result environment
     */
    @PostConstruct
    public void init() {
        ExportCheckResult.initEnvironment(properties.getDataPath());
    }

    /**
     * Enable verification service
     *
     * @param checkMode check Mode
     */
    @Statistical(name = "CheckServiceStart")
    @Override
    public String start(CheckMode checkMode) {
        if (STARTED.compareAndSet(false, true)) {
            endpointMetaDataManager.load();
            tableStatusRegister.selfCheck();
            log.info(CheckMessage.CHECK_SERVICE_STARTING, checkMode.getCode());
            try {
                CHECK_MODE_REF.set(checkMode);
                if (Objects.equals(CheckMode.FULL, checkMode)) {
                    startCheckFullMode();
                    // Wait for the task construction to complete, and start the task polling thread
                    startCheckPollingThread();
                } else {
                    startCheckIncrementMode();
                }
            } catch (CheckingException ex) {
                cleanCheck();
                throw new CheckingException(ex.getMessage());
            }
        } else {
            String message = String.format(CheckMessage.CHECK_SERVICE_START_ERROR, checkMode.getDescription());
            log.error(message);
            cleanCheck();
            throw new CheckingException(message);
        }
        return String.format(START_MESSAGE, PROCESS_SIGNATURE.get(), JsonObjectUtil.formatTime(LocalDateTime.now()));
    }

    interface CheckMessage {
        /**
         * Verify the startup message template
         */
        String CHECK_SERVICE_STARTING = "check service is starting, start check mode is [{}]";

        /**
         * Verify the startup message template
         */
        String CHECK_SERVICE_START_ERROR = "check service is running, current check mode is [%s] , exit.";
    }

    /**
     * Turn on full calibration mode
     */
    private void startCheckFullMode() {
        String processNo = IdGenerator.nextId36();
        log.info("check full mode : query meta data from db schema (source and sink )");
        // Source endpoint task construction
        final List<ExtractTask> extractTasks = feignClientService.buildExtractTaskAllTables(Endpoint.SOURCE, processNo);
        extractTasks.forEach(task -> log
            .debug("check full mode : build extract task source {} : {}", processNo, JSON.toJSONString(task)));
        // Sink endpoint task construction
        feignClientService.buildExtractTaskAllTables(Endpoint.SINK, processNo, extractTasks);
        log.info("check full mode : build extract task sink {}", processNo);
        // Perform all tasks
        feignClientService.execExtractTaskAllTables(Endpoint.SOURCE, processNo);
        feignClientService.execExtractTaskAllTables(Endpoint.SINK, processNo);
        log.info("check full mode : exec extract task (source and sink ) {}", processNo);
        PROCESS_SIGNATURE.set(processNo);
    }

    /**
     * Data verification polling thread
     * It is used to monitor the completion status of data extraction tasks in real time.
     * When the status of a data extraction task changes to complete, start a data verification independent thread.
     * And start the current task to verify the data.
     */
    public void startCheckPollingThread() {
        if (Objects.nonNull(PROCESS_SIGNATURE.get()) && Objects.equals(CHECK_MODE_REF.getAcquire(), CheckMode.FULL)) {
            ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
            endpointMetaDataManager.load();
            scheduledExecutor.scheduleWithFixedDelay(() -> {
                Thread.currentThread().setName(SELF_CHECK_POLL_THREAD_NAME);
                log.debug("check polling processNo={}", PROCESS_SIGNATURE.get());
                if (Objects.isNull(PROCESS_SIGNATURE.get())) {
                    throw new CheckingPollingException("process is empty,stop check polling");
                }
                // Check whether there is a table to complete data extraction
                if (isCheckWithSyncExtracting) {
                    checkTableWithSyncExtracting();
                } else {
                    checkTableWithExtractEnd();
                }
                completeProgressBar(scheduledExecutor);
            }, 5, 2, TimeUnit.SECONDS);
        }
    }

    /**
     * Enable incremental verification mode
     */
    private void startCheckIncrementMode() {
        //  Enable incremental verification mode - polling thread start
        if (Objects.equals(CHECK_MODE_REF.getAcquire(), CheckMode.INCREMENT)) {
            ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
            scheduledExecutor.scheduleWithFixedDelay(() -> {
                Thread.currentThread().setName(SELF_CHECK_POLL_THREAD_NAME);
                log.debug("check polling check mode=[{}]", CHECK_MODE_REF.get());
                //  Check whether there is a table to complete data extraction
                if (tableStatusRegister.hasExtractCompleted()) {
                    // Get the table name that completes data extraction
                    String tableName = tableStatusRegister.completedTablePoll();
                    if (Objects.isNull(tableName)) {
                        return;
                    }
                    Topic topic = feignClientService.getIncrementTopicInfo(Endpoint.SOURCE, tableName);

                    if (Objects.nonNull(topic)) {
                        log.info("kafka consumer topic=[{}]", topic.toString());
                        // Verify the data according to the table name and Kafka partition
                        dataCheckService.incrementCheckTableData(topic);
                    }
                    completeProgressBar(scheduledExecutor);
                }
                // The current cycle task completes the verification and resets the task status
                if (tableStatusRegister.isCheckCompleted()) {
                    log.info("The current cycle verification is completed, reset the task status!");
                    tableStatusRegister.rest();
                    feignClientService.cleanTask(Endpoint.SOURCE);
                    feignClientService.cleanTask(Endpoint.SINK);
                }
            }, 5, 2, TimeUnit.SECONDS);
        }
    }

    private void checkTableWithExtractEnd() {
        if (tableStatusRegister.isExtractCompleted() && CHECKING.get()) {
            log.info("check polling processNo={}, extract task complete. start checking....", PROCESS_SIGNATURE.get());
            CHECKING.set(false);
            endpointMetaDataManager.load();
            final List<String> checkTableList = endpointMetaDataManager.getCheckTableList();
            if (CollectionUtils.isEmpty(checkTableList)) {
                log.info("");
            }
            checkTableList.forEach(tableName -> {
                startCheckTableThread(tableName);
                ThreadUtil.sleep(100);
            });
        }
    }

    private void checkTableWithSyncExtracting() {
        if (!tableStatusRegister.isCheckCompleted()) {
            String tableName = tableStatusRegister.completedTablePoll();
            if (StringUtils.isNotEmpty(tableName)) {
                log.info("start checking thread of table {}", tableName);
                startCheckTableThread(tableName);
            }
        }
    }

    private void startCheckTableThread(String tableName) {
        Topic topic = feignClientService.queryTopicInfo(Endpoint.SOURCE, tableName);

        if (Objects.nonNull(topic)) {
            tableStatusRegister.initPartitionsStatus(tableName, topic.getPartitions());
            IntStream.range(0, topic.getPartitions()).forEach(idxPartition -> {
                log.info("kafka consumer topic=[{}] partitions=[{}]", topic.toString(), idxPartition);
                // Verify the data according to the table name and Kafka partition
                try {
                    final Future<?> future = dataCheckService.checkTableData(topic, idxPartition);
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    log.info("data check topic=[{}] partitions=[{}] error:", topic.toString(), idxPartition, e);
                }
            });
        }
    }

    private void completeProgressBar(ScheduledExecutorService scheduledExecutor) {
        Pair<Integer, Integer> process = tableStatusRegister.extractProgress();
        log.info("current check process has task total=[{}] , complete=[{}]", process.getSink(), process.getSource());

        // The current task completes the verification and resets the task status
        if (tableStatusRegister.isCheckCompleted()) {
            log.info("The current verification is completed, reset the task status!");
            if (isAutoCleanEnvironment) {
                log.info("The current cycle task completes the verification and resets the check environment");
                cleanCheck();
                feignClientService.cleanTask(Endpoint.SOURCE);
                feignClientService.cleanTask(Endpoint.SINK);
            }
            scheduledExecutor.shutdownNow();
        }
    }

    /**
     * Query the currently executed process number
     *
     * @return process number
     */
    @Override
    public String getCurrentCheckProcess() {
        return PROCESS_SIGNATURE.get();
    }

    /**
     * Clean up the verification environment
     */
    @Override
    public synchronized void cleanCheck() {
        final String processNo = PROCESS_SIGNATURE.get();
        cleanBuildTask(processNo);
        ThreadUtil.sleep(3000);
        CHECK_MODE_REF.set(null);
        PROCESS_SIGNATURE.set(null);
        STARTED.set(false);
        CHECKING.set(true);
        log.info("clear and reset the current verification service!");
    }

    /**
     * Increment Check Initialize configuration
     *
     * @param config Initialize configuration
     */
    @Override
    public void incrementCheckConfig(IncrementCheckConfig config) {
        feignClientService.configIncrementCheckEnvironment(Endpoint.SOURCE, config);
    }

    private void cleanBuildTask(String processNo) {
        try {
            feignClientService.cleanEnvironment(Endpoint.SOURCE, processNo);
            feignClientService.cleanEnvironment(Endpoint.SINK, processNo);
        } catch (CommonException ex) {
            log.error("ignore error:", ex);
        }
        tableStatusRegister.removeAll();
        log.info("The task registry of the verification service clears the data extraction task status information");
    }
}
