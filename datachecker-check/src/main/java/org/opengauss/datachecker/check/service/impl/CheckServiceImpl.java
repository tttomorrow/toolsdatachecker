package org.opengauss.datachecker.check.service.impl;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.check.cache.TableStatusRegister;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.modules.check.DataCheckService;
import org.opengauss.datachecker.check.service.CheckService;
import org.opengauss.datachecker.common.entry.check.IncrementCheckConifg;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.exception.CheckingPollingException;
import org.opengauss.datachecker.common.util.IdWorker;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
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
     * 校验任务启动标志
     * <p>
     * 无论是全量校验和增量校验，同一时间内只能执行一个。
     * 只有本地全量或者增量校验执行完成后，即{@code STARTED}==false时，才可以执行下一个。
     * 否则直接退出，等待当前校验流程执行完毕，自动退出。
     * <p>
     * 暂时不提供强制退出当前校验流程方法。
     */
    private static final AtomicBoolean STARTED = new AtomicBoolean(false);

    /**
     * 进程签名  后期是否删除进程签名逻辑
     */
    @Deprecated
    private static final AtomicReference<String> PROCESS_SIGNATURE = new AtomicReference<>();

    /**
     * 校验模式
     */
    private static final AtomicReference<CheckMode> CHECK_MODE_REF = new AtomicReference<>();

    /**
     * 校验轮询线程名称
     */
    private static final String SELF_CHECK_POLL_THREAD_NAME = "check-polling-thread";

    /**
     * 单线程定时任务 - 执行校验轮询线程 Thread.name={@value SELF_CHECK_POLL_THREAD_NAME}
     */
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    private final ThreadPoolExecutor singleThreadExecutor = ThreadUtil.newSingleThreadExecutor();

    @Autowired
    private FeignClientService feignClientService;

    @Autowired
    private TableStatusRegister tableStatusRegister;

    @Resource
    private DataCheckService dataCheckService;

    /**
     * 开启校验服务
     *
     * @param checkMode 校验方式
     */
    @Override
    public String start(CheckMode checkMode) {
        if (STARTED.compareAndSet(false, true)) {
            log.info("check service is starting, start check mode is [{}]", checkMode.getCode());
            CHECK_MODE_REF.set(checkMode);
            if (Objects.equals(CheckMode.FULL, checkMode)) {
                startCheckFullMode();
                // 等待任务构建完成，开启任务轮询线程
                startCheckPollingThread();
            } else {
                startCheckIncrementMode();
            }
        } else {
            String message = String.format("check service is running, current check mode is [%s] , exit.", checkMode.getDescription());
            log.error(message);
            throw new CheckingException(message);
        }
        return PROCESS_SIGNATURE.get();
    }

    /**
     * 开启全量校验模式
     */
    private void startCheckFullMode() {
        String processNo = IdWorker.nextId36();
        // 元数据信息查询
        feignClientService.queryMetaDataOfSchema(Endpoint.SOURCE);
        feignClientService.queryMetaDataOfSchema(Endpoint.SINK);
        log.info("check full mode : query meta data from db schema (source and sink )");
        // 源端任务构建
        final List<ExtractTask> extractTasks = feignClientService.buildExtractTaskAllTables(Endpoint.SOURCE, processNo);
        extractTasks.forEach(task -> log.debug("check full mode : build extract task source {} : {}", processNo, JSON.toJSONString(task)));
        // 宿端任务构建
        feignClientService.buildExtractTaskAllTables(Endpoint.SINK, processNo, extractTasks);
        log.info("check full mode : build extract task sink {}", processNo);
        // 构建任务执行
        feignClientService.execExtractTaskAllTables(Endpoint.SOURCE, processNo);
        feignClientService.execExtractTaskAllTables(Endpoint.SINK, processNo);
        log.info("check full mode : exec extract task (source and sink ) {}", processNo);
        PROCESS_SIGNATURE.set(processNo);
    }

    /**
     * /**
     * 数据校验轮询线程
     * 用于实时监测数据抽取任务的完成状态。
     * 当某一数据抽取任务状态变更为完成时，启动一个数据校验独立线程。并开启当前任务，进行数据校验。
     */
    public void startCheckPollingThread() {
        if (Objects.nonNull(PROCESS_SIGNATURE.get()) && Objects.equals(CHECK_MODE_REF.getAcquire(), CheckMode.FULL)) {
            scheduledExecutor.scheduleWithFixedDelay(() -> {
                Thread.currentThread().setName(SELF_CHECK_POLL_THREAD_NAME);
                log.debug("check polling processNo={}", PROCESS_SIGNATURE.get());
                if (Objects.isNull(PROCESS_SIGNATURE.get())) {
                    throw new CheckingPollingException("process is empty,stop check polling");
                }
                // 是否有表数据抽取完成
                if (tableStatusRegister.hasExtractComplated()) {
                    // 获取数据抽取完成表名
                    String tableName = tableStatusRegister.complatedTablePoll();
                    if (Objects.isNull(tableName)) {
                        return;
                    }
                    Topic topic = feignClientService.queryTopicInfo(Endpoint.SOURCE, tableName);

                    if (Objects.nonNull(topic)) {
                        IntStream.range(0, topic.getPartitions()).forEach(idxPartition -> {
                            log.info("kafka consumer topic=[{}] partitions=[{}]", topic.toString(), idxPartition);
                            // 根据表名称 和kafka分区进行数据校验
                            dataCheckService.checkTableData(topic, idxPartition);
                        });
                    }
                    complateProgressBar();
                }
            }, 0, 1, TimeUnit.SECONDS);
        }
    }


    private void complateProgressBar() {
        singleThreadExecutor.submit(() -> {
            Thread.currentThread().setName("complated-process-bar");
            int total = tableStatusRegister.getKeys().size();
            int complated = tableStatusRegister.complateSize();
            log.info("current check process has task total=[{}] , complate=[{}]", total, complated);
        });
    }

    /**
     * 开启增量校验模式
     */
    private void startCheckIncrementMode() {
        //  开启增量校验模式-轮询线程启动
        if (Objects.equals(CHECK_MODE_REF.getAcquire(), CheckMode.INCREMENT)) {
            scheduledExecutor.scheduleWithFixedDelay(() -> {
                Thread.currentThread().setName(SELF_CHECK_POLL_THREAD_NAME);
                log.debug("check polling check mode=[{}]", CHECK_MODE_REF.get());
                // 是否有表数据抽取完成
                if (tableStatusRegister.hasExtractComplated()) {
                    // 获取数据抽取完成表名
                    String tableName = tableStatusRegister.complatedTablePoll();
                    if (Objects.isNull(tableName)) {
                        return;
                    }
                    Topic topic = feignClientService.getIncrementTopicInfo(Endpoint.SOURCE, tableName);

                    if (Objects.nonNull(topic)) {
                        log.info("kafka consumer topic=[{}]", topic.toString());
                        // 根据表名称 和kafka分区进行数据校验
                        dataCheckService.incrementCheckTableData(topic);
                    }
                    complateProgressBar();
                }
                // 当前周期任务完成校验，重置任务状态
                if (tableStatusRegister.isCheckComplated()) {
                    log.info("当前周期校验完成，重置任务状态！");
                    tableStatusRegister.rest();
                    feignClientService.cleanTask(Endpoint.SOURCE);
                    feignClientService.cleanTask(Endpoint.SINK);
                }
            }, 0, 1, TimeUnit.SECONDS);
        }
    }

    /**
     * 查询当前执行的进程号
     *
     * @return 进程号
     */
    @Override
    public String getCurrentCheckProcess() {
        return PROCESS_SIGNATURE.get();
    }

    /**
     * 清理校验环境
     */
    @Override
    public synchronized void cleanCheck() {
        cleanBuildedTask();
        ThreadUtil.sleep(3000);
        CHECK_MODE_REF.set(null);
        PROCESS_SIGNATURE.set(null);
        STARTED.set(false);
        log.info("清除当前校验服务标识！");
        log.info("重置校验服务启动标识！");
    }

    @Override
    public void incrementCheckConifg(IncrementCheckConifg incrementCheckConifg) {
        feignClientService.configIncrementCheckEnvironment(Endpoint.SOURCE, incrementCheckConifg);
    }

    private void cleanBuildedTask() {
        try {
            feignClientService.cleanEnvironment(Endpoint.SOURCE, PROCESS_SIGNATURE.get());
            feignClientService.cleanEnvironment(Endpoint.SINK, PROCESS_SIGNATURE.get());
        } catch (RuntimeException ex) {
            log.error("ignore error:", ex);
        }
        tableStatusRegister.removeAll();
        log.info("数据抽取任务清除 ");
    }
}
