/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.rc;

import com.yahoo.gondola.Clock;
import com.yahoo.gondola.Gondola;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RcClock implements Clock {
    final static Logger logger = LoggerFactory.getLogger(Gondola.class);

    final ReentrantLock lock = new ReentrantLock();
    final Condition sleepCond = lock.newCondition();
    Map<Long, Waiter> waiters = new ConcurrentHashMap<>();

    // Used to wake sleepers
    int generation;

    // The virtual current time
    long now;

    public RcClock(Gondola gondola, String hostId) {
    }

    public void start() {
        now = 0;
    }

    public boolean stop() {
        generation++;

        lock.lock();
        try {
            // Wake all sleepers
            sleepCond.signalAll();
        } finally {
            lock.unlock();
        }

        // Wake waiters that have timed out
        waiters.forEach((k, w) -> w.signalAll());
        waiters.clear();
        return true;
    }
    
    /******************** methods *********************/

    @Override
    public long now() {
        return now;
    }

    @Override
    public void sleep(long delay) throws InterruptedException {
        int gen = generation;
        long end = now + delay;
        lock.lock();
        try {
            while (gen == generation && now < end) {
                sleepCond.await();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void awaitCondition(Lock lock, Condition cond, long timeMs) throws InterruptedException {
        waiters.put(Thread.currentThread().getId(), new Waiter(lock, cond, now + timeMs));
        cond.await();
        waiters.remove(Thread.currentThread().getId());
    }

    public void tick(int time) {
        now += time;
        lock.lock();
        try {
            // Wake all sleepers
            sleepCond.signalAll();
        } finally {
            lock.unlock();
        }

        // Wake waiters that have timed out
        for (Waiter w : waiters.values()) {
            if (now >= w.end) {
                w.signalAll();
            }
        }
    }

    class Waiter {
        Lock lock;
        Condition condition;
        long end;

        Waiter(Lock lock, Condition condition, long end) {
            this.lock = lock;
            this.condition = condition;
            this.end = end;
        }

        public void signalAll() {
            lock.lock();
            condition.signalAll();
            lock.unlock();
        }
    }
}
