package org.opengauss.datachecker.check.service;


import org.opengauss.datachecker.common.entry.check.IncrementCheckConifg;
import org.opengauss.datachecker.common.entry.enums.CheckMode;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
public interface CheckService {

    /**
     * 开启校验服务
     *
     * @param checkMode 校验方式
     * @return 进程号
     */
    String start(CheckMode checkMode);

    /**
     * 查询当前执行的进程号
     *
     * @return 进程号
     */
    String getCurrentCheckProcess();

    /**
     * 清理校验环境
     */
    void cleanCheck();

    /**
     * 增量校验配置初始化
     *
     * @param incrementCheckConifg 初始化配置
     */
    void incrementCheckConifg(IncrementCheckConifg incrementCheckConifg);
}
