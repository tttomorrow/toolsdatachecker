package org.opengauss.datachecker.extract.config;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.enums.ResultEnum;
import org.opengauss.datachecker.common.exception.*;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.servlet.http.HttpServletRequest;

@Slf4j
@RestControllerAdvice
public class GlobalExtractExceptionHandler extends GlobalCommonExceptionHandler {
    private static final String MESSAGE_TEMPLATE = "path:{}, queryParam:[{}] , error:";

    /**
     * service exception handing
     */
    @ExceptionHandler(value = ExtractException.class)
    public Result extractException(HttpServletRequest request, ExtractException exception) {
        logError(request, exception);
        return Result.fail(ResultEnum.EXTRACT, exception.getMessage());
    }

    @ExceptionHandler(value = CreateTopicException.class)
    public Result createTopicException(HttpServletRequest request, CreateTopicException exception) {
        logError(request, exception);
        return Result.fail(ResultEnum.CREATE_TOPIC, exception.getMessage());
    }

    @ExceptionHandler(value = ProcessMultipleException.class)
    public Result processMultipleException(HttpServletRequest request, ProcessMultipleException exception) {
        logError(request, exception);
        return Result.fail(ResultEnum.PROCESS_MULTIPLE, exception.getMessage());
    }

    @ExceptionHandler(value = TaskNotFoundException.class)
    public Result taskNotFoundException(HttpServletRequest request, TaskNotFoundException exception) {
        logError(request, exception);
        return Result.fail(ResultEnum.TASK_NOT_FOUND, exception.getMessage());
    }

    @ExceptionHandler(value = TableNotExistException.class)
    public Result tableNotExistException(HttpServletRequest request, TableNotExistException exception) {
        logError(request, exception);
        return Result.fail(ResultEnum.TABLE_NOT_FOUND, exception.getMessage());
    }

    @ExceptionHandler(value = DebeziumConfigException.class)
    public Result debeziumConfigException(HttpServletRequest request, DebeziumConfigException exception) {
        logError(request, exception);
        return Result.fail(ResultEnum.DEBEZIUM_CONFIG_ERROR, exception.getMessage());
    }

    private void logError(HttpServletRequest request, ExtractException exception) {
        log.error(MESSAGE_TEMPLATE, request.getRequestURI(), request.getQueryString(), exception);
    }
}
