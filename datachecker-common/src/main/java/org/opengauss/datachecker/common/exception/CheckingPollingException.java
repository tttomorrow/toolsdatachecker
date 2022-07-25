package org.opengauss.datachecker.common.exception;

/**
 * 校验服务 校验轮询异常
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class CheckingPollingException extends CheckingException {

    public CheckingPollingException(String message) {
        super(message);
    }

}
