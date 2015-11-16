/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.impl;

import com.yahoo.gondola.Clock;
import com.yahoo.gondola.Gondola;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class SystemClock implements Clock {
    final static Logger logger = LoggerFactory.getLogger(SystemClock.class);

    public SystemClock(Gondola gondola, String hostId) {
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public boolean stop() {
        return true;
    }

    @Override
    public long now() {
        return System.currentTimeMillis();
    }

    @Override
    public void sleep(long delay) throws InterruptedException {
        Thread.sleep(delay);
    }

    @Override
    public void awaitCondition(Lock lock, Condition cond, long timeMs) throws InterruptedException {
        cond.await(timeMs, TimeUnit.MILLISECONDS);
    }
}
