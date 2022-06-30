package org.opengauss.datachecker.check.modules.task;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.check.cache.TableStatusRegister;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
@Slf4j
@Service
public class TaskManagerServiceImpl implements TaskManagerService {

    @Autowired
    private TableStatusRegister tableStatusRegister;

    /**
     * 刷新指定任务的数据抽取表执行状态
     *
     * @param tableName 表名称
     * @param endpoint  端点类型 {@link org.opengauss.datachecker.common.entry.enums.Endpoint}
     */
    @Override
    public void refushTableExtractStatus(String tableName, Endpoint endpoint) {
        log.info("check server refush endpoint=[{}]  extract tableName=[{}] status=[{}]  ", endpoint.getDescription(), tableName, endpoint.getCode());
        tableStatusRegister.update(tableName, endpoint.getCode());
    }

    /**
     * 初始化任务状态
     *
     * @param tableNameList 表名称列表
     */
    @Override
    public void initTableExtractStatus(List<String> tableNameList) {
        if (tableStatusRegister.isEmpty() || tableStatusRegister.isCheckComplated()) {
            cleanTaskStatus();
            tableStatusRegister.init(new HashSet<>(tableNameList));
            log.info("check server init extract tableNameList=[{}] status= ", JSON.toJSONString(tableNameList));
        } else {
            //上次校验流程正在执行，不能重新初始化表校验状态数据！
            throw new CheckingException("The last verification process is being executed, and the table verification status data cannot be reinitialized!");
        }

    }

    /**
     * 清理任务状态信息
     */
    @Override
    public void cleanTaskStatus() {
        tableStatusRegister.removeAll();
    }
}
