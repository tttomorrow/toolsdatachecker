package org.opengauss.datachecker.common.exception;

/**
 * 校验服务 数据量产生过大差异，无法进行校验。
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class LargeDataDiffException extends CheckingException {

    public LargeDataDiffException(String message) {
        super(message);
    }

}
