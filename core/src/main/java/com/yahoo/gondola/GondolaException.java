/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola;

/**
 * General exception thrown by Gondola methods when an error occurs.
 */
public class GondolaException extends Exception {

    /**
     * This enum is used to represent all Gondola errors.
     */
    public enum Code {
        /** Used for any generic error condition. The message should provide details of the error. */
        ERROR(""),
        NOT_LEADER("The leader is %s"),
        SAME_SHARD("This slave (%d) and the master (%d) cannot be in the same shard (%s)"),
        SLAVE_MODE("This operation is not allowed while the member (%d) is in slave mode");

        private String messageTemplate;

        Code(String messageTemplate) {
            this.messageTemplate = messageTemplate;
        }

        public String messageTemplate() {
            return messageTemplate;
        }
    }

    private Code code;

    public GondolaException(Throwable cause) {
        super(cause.getMessage(), cause);
        this.code = Code.ERROR;
    }

    public GondolaException(String message) {
        super(message);
        this.code = Code.ERROR;
    }

    public GondolaException(String message, Throwable cause) {
        super(message, cause);
        this.code = Code.ERROR;
    }

    public GondolaException(Code code, Object... args) {
        super(String.format(code.messageTemplate(), args));
        this.code = code;
    }

    public Code getCode() {
        return code;
    }
}
