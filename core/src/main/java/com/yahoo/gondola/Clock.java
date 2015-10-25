/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * This interface is used for any time-related functions.
 * The purpose of this interface is to make it possible to be tests that mock time.
 */
public interface Clock extends Stoppable {

    public void start() throws Exception;

    public void stop();
    
    /**
     * Returns the current time. Equivalent to System.currentTimeMillis().
     *
     * @return The current time in millieconds from Epoch.
     */
    public long now();

    /**
     * Delays the caller by delayMs milliseconds. Equivalent to Thread.sleep().
     *
     * @param delayMs The number of milliseconds to delay the current thread.
     */
    public void sleep(long delayMs) throws InterruptedException;

    /**
     * Equivlant to:
     *   lock.lock();
     *   cond.await(TimeUnit.MILLISECONDS, timeMs);
     *   lock.unlock();
     *
     * @param lock The lock that protects cond.
     * @param cond A Condition created from lock.
     * @param timeMs The timeout in milliseconds.
     */
    public void awaitCondition(Lock lock, Condition cond, long timeMs) throws InterruptedException;
}
