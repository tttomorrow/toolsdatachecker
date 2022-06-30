package org.opengauss.datachecker.common.exception;

import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.springframework.lang.NonNull;

/**
 * 调度客户端异常
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class DispatchClientException extends FeignClientException {

    /**
     * 调度客户端异常
     *
     * @param endpoint 端点
     * @param message  异常信息
     */
    public DispatchClientException(@NonNull Endpoint endpoint, String message) {
        super(endpoint, message);
    }

}
