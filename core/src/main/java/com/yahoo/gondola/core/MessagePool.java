/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.core;

import com.yahoo.gondola.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * The type Message pool.
 */
public class MessagePool {
    final static Logger logger = LoggerFactory.getLogger(MessagePool.class);

    Config config;
    Stats stats;

    // Number of created message objects
    int createdCount;

    // Pool of message objects
    Queue<Message> pool = new ConcurrentLinkedQueue<>();

    // All references in this queue represent gc'ed messages. Messages should never be gc'ed.
    ReferenceQueue<Message> leakQueue = new ReferenceQueue<>();
    Queue<PhantomRef<Message>> phantomRefs = new ConcurrentLinkedQueue<>();

    // Config variables
    int warnThreshold;
    boolean leakTracing;

    public MessagePool(Config config, Stats stats) {
        this.config = config;
        this.stats = stats;

        config.registerForUpdates(configListener);
    }

    /*
     * Called at the time of registration and whenever the config file changes.
     */
    Consumer<Config> configListener = config1 -> {
        warnThreshold = config1.getInt("gondola.message_pool_warn_threshold");
        leakTracing = config1.getBoolean("gondola.tracing.message_leak");
    };

    public int size() {
        return pool.size();
    }

    /**
     * Blocks until there are messages in the pool.
     * The refCount for the return message is 1.
     * The bytebuffer is ready for writing.
     */
    public Message checkout() {
        Message message = pool.poll();
        if (message == null) {
            if (createdCount >= warnThreshold) {
                if (leakTracing) {
                    PhantomRef<?extends Message> pr = (PhantomRef<?extends Message>) leakQueue.poll();
                    if (pr != null) {
                        logger.error("Message leaked: id={} type={} refCount={} size={}",
                                pr.id, pr.type, pr.refCount.get(), pr.size);
                        pr.clear();
                        phantomRefs.remove(pr);
                    }
                }

                // Try again
                message = pool.poll();
                /*
                if (message == null) {
                    logger.warn(
                    "The number of created messages ({}) has passed the threshold of {}. Current pool size={}",
                                createdCount, warnThreshold, pool.size());
                }
                */
            }
        }
        if (message == null) {
            message = new Message(config, this, stats);

            // Create a phantom reference to determine if the message is leaked
            if (config.getBoolean("gondola.tracing.message_leak")) {
                phantomRefs.add(new PhantomRef<>(message, leakQueue));
            }
            createdCount++;
        }

        // Set ref count to 1
        message.acquire();
        message.byteBuffer.clear();
        return message;
    }

    void checkin(Message message) {
        if (message.refCount.get() != 0) {
            throw new IllegalStateException(String.format("Message type=%d ref count is not zero when checked in: %d",
                                                          message.getType(), message.refCount.get()));
        }

        pool.add(message);
    }

    /**
     * Used for leak detection.
     */
    class PhantomRef<T extends Message> extends PhantomReference<T> {
        int id;
        int size;
        int type;
        AtomicInteger refCount;

        PhantomRef(T message, ReferenceQueue<Message> queue) {
            super(message, queue);
            id = message.id;
            size = message.size;
            type = message.getType();
            refCount = message.refCount;
        }
    }
}
