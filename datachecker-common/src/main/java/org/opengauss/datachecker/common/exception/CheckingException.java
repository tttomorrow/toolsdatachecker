package org.opengauss.datachecker.common.exception;

import java.util.Objects;

public class CheckingException extends RuntimeException {

    private final String msg;

    public CheckingException(String message) {
        this.msg = message;
    }
    @Override
    public String getMessage() {
        String message = super.getMessage();
        if (Objects.isNull(message)) {
            return this.msg;
        }
        return this.msg + message;
    }
}
