/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.core;

import com.yahoo.gondola.GondolaException;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * Actions executed by the CoreMember main loop.
 */
public class CoreMemberActionQueue {

    // Contains action requests from other threads
    Queue<Action> queue = new ConcurrentLinkedQueue<>();

    /**
     * The serialized Action execute by MainLoop.
     */
    public enum Type {
        BECOME_FOLLOWER,
        UPDATE_SAVED_INDEX,
        UPDATE_STORAGE_INDEX,
        EXECUTE,
    }

    static class Action {
        Type type;
        FutureTask<Void> futureTask;

        Action(Type type) {
            this.type = type;
        }

        Action(FutureTask<Void> futureTask) {
            this.type = Type.EXECUTE;
            this.futureTask = futureTask;
        }
    }

    public void becomeFollower() {
        queue.add(new Action(Type.BECOME_FOLLOWER));
    }

    public void updateSavedIndex() {
        queue.add(new Action(Type.UPDATE_SAVED_INDEX));
    }

    public void updateStorageIndex() {
        queue.add(new Action(Type.UPDATE_STORAGE_INDEX));
    }

    public void execute(Runnable runnable) throws GondolaException, InterruptedException {
        FutureTask<Void> task = new FutureTask<>(runnable, null);
        queue.add(new Action(task));
        try {
            task.get();
        } catch (ExecutionException e) {
            throw new GondolaException(e);
        }
    }
}
