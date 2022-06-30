package org.opengauss.datachecker.common.exception;

/**
 * 校验服务 配置源端和宿端地址进行约束性检查：源端和宿端地址不能重复
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class CheckingAddressConflictException extends CheckingException {

    public CheckingAddressConflictException(String message) {
        super(message);
    }

}
