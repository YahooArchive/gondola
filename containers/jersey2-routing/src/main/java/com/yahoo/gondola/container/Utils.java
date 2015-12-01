/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Utility class.
 */
public class Utils {

    /**
     * Polling with timeout utility function, accept a boolean supplier that throws Exception. It'll retry until the
     * retryCount reached or there's timeout.
     *
     * @param timeoutMs -1 means no limitation, 0 means no wait.
     * @return true if success, false if timeout reached.
     */
    public static boolean pollingWithTimeout(CheckedBooleanSupplier supplier, long waitTimeMs, long timeoutMs)
        throws InterruptedException, ExecutionException {
        return pollingWithTimeout(supplier, waitTimeMs, timeoutMs, null, null);
    }


    public static boolean pollingWithTimeout(CheckedBooleanSupplier supplier, long waitTimeMs, long timeoutMs,
                                             Lock lock, Condition condition)
        throws ExecutionException, InterruptedException {
        long start = System.currentTimeMillis();
        // TODO: default timeout MS should be provided by upper layer.
        if (timeoutMs < 0) {
            waitTimeMs = 1000;
        }
        try {
            while (timeoutMs <= 0 || System.currentTimeMillis() - start < timeoutMs) {
                lock(lock);
                try {
                    if (supplier.getAsBoolean()) {
                        return true;
                    }
                    long remain = timeoutMs - (System.currentTimeMillis() - start);
                    if (timeoutMs == 0) {
                        break;
                    }
                    long timeout = timeoutMs == -1 || waitTimeMs < remain || remain <= 0 ? waitTimeMs : remain;
                    wait(lock, condition, timeout);
                } finally {
                    unlock(lock);
                }
            }
            return false;
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    private static void wait(Lock lock, Condition condition, long timeout) throws InterruptedException {
        if (lock != null) {
            condition.await(timeout, TimeUnit.MILLISECONDS);
        } else {
            Thread.sleep(timeout);
        }
    }

    private static void unlock(Lock lock) {
        if (lock != null) {
            lock.unlock();
        }
    }

    private static void lock(Lock lock) {
        if (lock != null) {
            lock.lock();
        }
    }

    /**
     * A functional interface that return boolean value and throw Exception if any error.
     */
    @FunctionalInterface
    public interface CheckedBooleanSupplier {

        boolean getAsBoolean() throws Exception;
    }
}
