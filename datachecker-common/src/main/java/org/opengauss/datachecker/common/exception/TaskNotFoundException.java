package org.opengauss.datachecker.common.exception;

import org.opengauss.datachecker.common.entry.enums.Endpoint;

/**
 * 数据抽取服务，未找到待执行抽取任务
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class TaskNotFoundException extends ExtractException {
    private static final String ERROR_MESSAGE = "task %s is not found,please checking something error!";
    private static final String ERROR_ENDPOINT_MESSAGE = "endpoint [%s] and process[%s] task is empty!";

    public TaskNotFoundException(Endpoint endpoint, long process) {
        super(String.format(ERROR_ENDPOINT_MESSAGE, endpoint.getDescription(), process));
    }

    public TaskNotFoundException(String taskName) {
        super(String.format(ERROR_MESSAGE, taskName));
    }

}
