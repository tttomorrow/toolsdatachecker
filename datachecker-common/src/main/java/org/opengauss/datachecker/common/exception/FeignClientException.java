package org.opengauss.datachecker.common.exception;

import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.springframework.lang.NonNull;

/**
 * 工具FeignClient 调用异常
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class FeignClientException extends RuntimeException {

    public FeignClientException(@NonNull Endpoint endpoint, String message) {
        super(endpoint.getDescription() + " " + message);
    }

}
