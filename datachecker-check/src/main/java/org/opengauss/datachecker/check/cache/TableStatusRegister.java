package org.opengauss.datachecker.check.cache;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.exception.ExtractException;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.validation.constraints.NotEmpty;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
@Slf4j
@Service
public class TableStatusRegister implements Cache<String, Integer> {

    /**
     * 任务状态缓存默认值
     */
    private static final int TASK_STATUS_DEFAULT_VALUE = 0;
    /**
     * 任务状态 源端 和宿端均完成数据抽取
     */
    public static final int TASK_STATUS_COMPLATED_VALUE = 3;
    /**
     * 任务状态 校验服务已进行当前任务校验
     */
    public static final int TASK_STATUS_COMSUMER_VALUE = 7;
    /**
     * 状态自检线程名称
     */
    private static final String SELF_CHECK_THREAD_NAME = "task-register-self-check-thread";

    /**
     * 数据抽取任务对应表 执行状态缓存
     * {@code tableStatusCache} : key 为数据抽取表名称
     * {@code tableStatusCache} : value 为数据抽取表完成状态
     * value 值初始化状态为 0
     * 源端完成表识为 1 则更新当前表缓存状态为 value = value | 1
     * 宿端完成表识为 2 则更新当前表缓存状态为 value = value | 2
     * 数据校验标识为 4 则更新当前表缓存状态为 value = value | 4
     */
    private static final Map<String, Integer> TABLE_STATUS_CACHE = new ConcurrentHashMap<>();

    /**
     * 单线程定时任务
     */
    private static final ScheduledExecutorService SCHEDULED_EXECUTOR = Executors.newSingleThreadScheduledExecutor();
    /**
     * 完成表集合
     */
    private static final Set<String> COMPLATED_TABLE = new HashSet<>();
    /**
     * {@code complatedTableQueue} poll消费记录
     */
    private static final Set<String> CONSUMER_COMPLATED_TABLE = new HashSet<>();

    /**
     *
     */
    private static final BlockingDeque<String> COMPLATED_TABLE_QUEUE = new LinkedBlockingDeque<>();

    /**
     * 服务启动恢复缓存信息。根据持久化缓存数据，恢复历史数据
     * 扫描指定位置的缓存文件，解析JSON字符串反序列化当前缓存数据
     */
    @PostConstruct
    public void recover() {
        selfCheck();
        //  扫描指定位置的缓存文件，解析JSON字符串反序列化当前缓存数据
    }

    /**
     * 开启并执行自检线程
     */
    public void selfCheck() {
        SCHEDULED_EXECUTOR.scheduleWithFixedDelay(() -> {
            Thread.currentThread().setName(SELF_CHECK_THREAD_NAME);
            doCheckingStatus();

        }, 0, 1, TimeUnit.SECONDS);
    }

    /**
     * 完成数据抽取任务数量 和 已消费的完成任务数量一致，且大于0时，认为本次校验服务已完成
     *
     * @return
     */
    public boolean isCheckComplated() {
        return CONSUMER_COMPLATED_TABLE.size() > 0 && CONSUMER_COMPLATED_TABLE.size() == COMPLATED_TABLE.size();
    }

    /**
     * 增量任务状态重置
     */
    public void rest() {
        CONSUMER_COMPLATED_TABLE.clear();
        COMPLATED_TABLE.clear();
        init(TABLE_STATUS_CACHE.keySet());
    }

    public int complateSize() {
        return COMPLATED_TABLE.size();
    }

    public boolean isEmpty() {
        return CONSUMER_COMPLATED_TABLE.size() == 0 && COMPLATED_TABLE.size() == 0;
    }

    public boolean hasExtractComplated() {
        return COMPLATED_TABLE.size() > 0;
    }

    /**
     * 初始化缓存 并给键值设置默认值
     *
     * @param keys
     * @return
     */
    @Override
    public void init(@NotEmpty Set<String> keys) {
        keys.forEach(key -> {
            TABLE_STATUS_CACHE.put(key, TASK_STATUS_DEFAULT_VALUE);
        });
    }

    /**
     * 添加表状态对到缓存
     *
     * @param key   键
     * @param value 值
     * @return 返回任务状态
     */
    @Override
    public void put(String key, Integer value) {
        if (TABLE_STATUS_CACHE.containsKey(key)) {
            // 当前key已存在不能重复添加
            throw new ExtractException("The current key= " + key + " already exists and cannot be added repeatedly");
        }
        TABLE_STATUS_CACHE.put(key, value);
    }

    /**
     * 根据key查询缓存
     *
     * @param key 缓存key
     * @return 缓存value
     */
    @Override
    public Integer get(String key) {
        return TABLE_STATUS_CACHE.getOrDefault(key, -1);
    }

    /**
     * 获取缓存Key集合
     *
     * @return Key集合
     */
    @Override
    public Set<String> getKeys() {
        return TABLE_STATUS_CACHE.keySet();
    }

    /**
     * 更新缓存数据
     *
     * @param key   缓存key
     * @param value 缓存value
     * @return 更新后的缓存value
     */
    @Override
    public Integer update(String key, Integer value) {
        if (!TABLE_STATUS_CACHE.containsKey(key)) {
            log.error("current key={} does not exist", key);
            return 0;
        }
        Integer odlValue = TABLE_STATUS_CACHE.get(key);
        TABLE_STATUS_CACHE.put(key, odlValue | value);
        return TABLE_STATUS_CACHE.get(key);
    }


    /**
     * 删除指定key缓存
     *
     * @param key key
     */
    @Override
    public void remove(String key) {
        TABLE_STATUS_CACHE.remove(key);
    }

    /**
     * 清除全部缓存
     */
    @Override
    public void removeAll() {
        COMPLATED_TABLE.clear();
        CONSUMER_COMPLATED_TABLE.clear();
        TABLE_STATUS_CACHE.clear();
        COMPLATED_TABLE_QUEUE.clear();
        log.info("table status register cache information clearing");
    }

    /**
     * 缓存持久化接口 将缓存信息持久化到本地
     * 将缓存信息持久化到本地的缓存文件，序列化为JSON字符串，保存到本地指定文件中
     */
    @Override
    public void persistent() {
    }

    /**
     * 返回并删除 已完成数据抽取任务的统计队列{@code complatedTableQueue} 头节点，
     * 如果队列为空，则返回{@code null}
     *
     * @return 返回队列头节点，如果队列为空，则返回{@code null}
     */
    public String complatedTablePoll() {
        return COMPLATED_TABLE_QUEUE.poll();
    }

    /**
     * 检查是否存在已完成数据抽取任务。若已完成，则返回true
     *
     * @return true 有已完成数据抽取的任务
     */
    private void doCheckingStatus() {
        Set<String> keys = TABLE_STATUS_CACHE.keySet();
        keys.forEach(tableName -> {
            final int taskStatus = TABLE_STATUS_CACHE.get(tableName);
            log.debug("check table=[{}] status=[{}] ", tableName, taskStatus);
            if (!COMPLATED_TABLE.contains(tableName)) {
                if (taskStatus == TableStatusRegister.TASK_STATUS_COMPLATED_VALUE) {
                    COMPLATED_TABLE.add(tableName);
                    COMPLATED_TABLE_QUEUE.add(tableName);
                    log.info("extract [{}] complated", tableName);
                }
            }

            if (!CONSUMER_COMPLATED_TABLE.contains(tableName)) {
                if (taskStatus == TableStatusRegister.TASK_STATUS_COMSUMER_VALUE) {
                    CONSUMER_COMPLATED_TABLE.add(tableName);
                    log.info("consumer [{}] complated", tableName);
                }
            }
        });
    }
}
