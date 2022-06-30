package org.opengauss.datachecker.check.config;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.enums.ResultEnum;
import org.opengauss.datachecker.common.exception.*;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.servlet.http.HttpServletRequest;

@Slf4j
@RestControllerAdvice
public class GlobalCheckingExceptionHandler extends GlobalCommonExceptionHandler {

    /**
     * 业务异常处理
     */
    @ExceptionHandler(value = CheckingException.class)
    public Result checkingException(HttpServletRequest request, CheckingException e) {
        log.error("path:{}, queryParam:{}message:{}", request.getRequestURI(), request.getQueryString(),
                e.getMessage(), e);
        logError(request, e);
        return Result.fail(ResultEnum.CHECKING, e.getMessage());
    }

    @ExceptionHandler(value = CheckingAddressConflictException.class)
    public Result checkingAddressConflictException(HttpServletRequest request, CheckingAddressConflictException e) {
        log.error("path:{}, queryParam:{}message:{}", request.getRequestURI(), request.getQueryString(),
                e.getMessage(), e);
        logError(request, e);
        return Result.fail(ResultEnum.CHECKING_ADDRESS_CONFLICT, e.getMessage());
    }

    @ExceptionHandler(value = CheckMetaDataException.class)
    public Result checkMetaDataException(HttpServletRequest request, CheckMetaDataException e) {
        log.error("path:{}, queryParam:{}message:{}", request.getRequestURI(), request.getQueryString(),
                e.getMessage(), e);
        logError(request, e);
        return Result.fail(ResultEnum.CHECK_META_DATA, e.getMessage());
    }

    @ExceptionHandler(value = LargeDataDiffException.class)
    public Result largeDataDiffException(HttpServletRequest request, LargeDataDiffException e) {
        log.error("path:{}, queryParam:{}message:{}", request.getRequestURI(), request.getQueryString(),
                e.getMessage(), e);
        logError(request, e);
        return Result.fail(ResultEnum.LARGE_DATA_DIFF, e.getMessage());
    }

    @ExceptionHandler(value = MerkleTreeDepthException.class)
    public Result merkleTreeDepthException(HttpServletRequest request, MerkleTreeDepthException e) {
        log.error("path:{}, queryParam:{}message:{}", request.getRequestURI(), request.getQueryString(),
                e.getMessage(), e);
        logError(request, e);
        return Result.fail(ResultEnum.MERKLE_TREE_DEPTH, e.getMessage());
    }
}
