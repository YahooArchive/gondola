/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import java.util.concurrent.ExecutionException;

public class Utils {

    /**
     * Polling with timeout utility function, accept a boolean supplier that throws Exception.
     * It'll retry until the retryCount reached or there's timeout.
     *
     * @param supplier
     * @param waitTimeMs
     * @param timeoutMs
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static boolean pollingWithTimeout(CheckedBooleanSupplier supplier, long waitTimeMs, long timeoutMs)
        throws InterruptedException, ExecutionException  {
        if (timeoutMs == -1) {
            waitTimeMs = 1000;
        }
        long start = System.currentTimeMillis();
        // TODO: default timeout MS should be provided by upper layer.
        if (timeoutMs == -1) {
            waitTimeMs = 1000;
        }
        try {
            while (timeoutMs == -1 || System.currentTimeMillis() - start < timeoutMs) {
                boolean success = supplier.getAsBoolean();
                if (success) {
                    return true;
                }
                long remain = timeoutMs - (System.currentTimeMillis() - start);
                Thread.sleep(timeoutMs == -1 || waitTimeMs < remain || remain <= 0 ? waitTimeMs : remain);
            }
            return false;
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    @FunctionalInterface
    public interface CheckedBooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }
}
