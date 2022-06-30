package org.opengauss.datachecker.common.exception;

/**
 * 当前实例正在执行数据抽取服务，不能重新开启新的校验。
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class ProcessMultipleException extends ExtractException {

    public ProcessMultipleException(String message) {
        super(message);
    }

}
