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
        NOT_LEADER("%s is not a leader");

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

    public GondolaException(Code code, String message) {
        super(message);
        this.code = code;
    }

    public GondolaException(Code code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }
}
