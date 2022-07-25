package org.opengauss.datachecker.common.exception;

import lombok.Getter;

@Getter
public class ExtractException extends RuntimeException {

    //数据抽取服务异常
    private String message = "Data extraction service exception";

    public ExtractException(String message) {
        this.message = message;
    }

    public ExtractException() {
    }
}
