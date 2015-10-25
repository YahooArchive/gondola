/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola;

/**
 * This interface is used by objects that create helper threads within their implementation.
 * The user of Stoppable components can start and stop these internal worker threads by called 
 * start() and stop() respectively. The methods start() and stop() are not thread-safe and should
 * only be called in exactly the correct sequence - start(), then stop(), then start(), etc.
 */
public interface Stoppable {
    /**
     * Start all threads needed by the object.
     *
     * @throw Exception Is thrown when some thread cannot be properly started. In this case, the state
     * of the instance is not known. To reuse the instance, call stop() to reset the state of the instance.
     */
    public void start() throws Exception;

    /**
     * Stops all threads in the object. After this call, it should be possible to call start() to restart
     * the threads. Can be called after a failed start() to try and clean up any 
     */
    public void stop();
}
