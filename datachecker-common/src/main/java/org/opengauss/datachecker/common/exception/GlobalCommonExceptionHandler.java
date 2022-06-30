package org.opengauss.datachecker.common.exception;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.enums.ResultEnum;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;


import javax.servlet.http.HttpServletRequest;

@Slf4j
public class GlobalCommonExceptionHandler {

    /**
     * 缺少必要的参数
     */
    @ExceptionHandler(value = MissingServletRequestParameterException.class)
    public Result missingParameterHandler(HttpServletRequest request, MissingServletRequestParameterException e) {
        this.logError(request, e);
        return Result.fail(ResultEnum.PARAM_MISSING);
    }

    /**
     * 参数类型不匹配
     */
    @ExceptionHandler(value = MethodArgumentTypeMismatchException.class)
    public Result methodArgumentTypeMismatchException(HttpServletRequest request, MethodArgumentTypeMismatchException e) {
        this.logError(request, e);
        return Result.fail(ResultEnum.PARAM_TYPE_MISMATCH);
    }

    /**
     * 不支持的请求方法
     */
    @ExceptionHandler(value = HttpRequestMethodNotSupportedException.class)
    public Result httpRequestMethodNotSupportedException(HttpServletRequest request, HttpRequestMethodNotSupportedException e) {
        this.logError(request, e);
        return Result.fail(ResultEnum.HTTP_REQUEST_METHOD_NOT_SUPPORTED_ERROR);
    }

    /**
     * 参数错误
     */
    @ExceptionHandler(value = IllegalArgumentException.class)
    public Result illegalArgumentException(HttpServletRequest request, IllegalArgumentException e) {
        this.logError(request, e);
        return Result.fail(ResultEnum.SERVER_ERROR_PRARM);
    }

    @ExceptionHandler(value = FeignClientException.class)
    public Result feignClientException(HttpServletRequest request, FeignClientException e) {
        this.logError(request, e);
        return Result.fail(ResultEnum.FEIGN_CLIENT);
    }

    @ExceptionHandler(value = DispatchClientException.class)
    public Result dispatchClientException(HttpServletRequest request, DispatchClientException e) {
        this.logError(request, e);
        return Result.fail(ResultEnum.DISPATCH_CLIENT);
    }

    /**
     * 其他异常统一处理
     */
    @ExceptionHandler(value = Exception.class)
    public Result exception(HttpServletRequest request, Exception e) {
        this.logError(request, e);
        return Result.fail(ResultEnum.SERVER_ERROR);
    }

    /**
     * 记录错误日志
     */
    protected void logError(HttpServletRequest request, Exception e) {
        log.error("path:{}, queryParam:{}, errorMessage:{}", request.getRequestURI(), request.getQueryString(), e.getMessage(), e);
    }
}
