/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.core;

/**
 * In the code, the term/index pair is called a 'Raft ID'.
 * This class is used to store both the term and index together in one object.
 * This class makes it possible to atomically update the two values without the need for a lock.
 */
public class Rid {
    public int term;
    public int index;

    Rid() {
    }

    Rid(int term, int index) {
        this.term = term;
        this.index = index;
    }

    void set(Rid other) {
        term = other.term;
        index = other.index;
    }

    void set(int term, int index) {
        this.term = term;
        this.index = index;
    }

    boolean equals(int term, int index) {
        return this.term == term && this.index == index;
    }
}
