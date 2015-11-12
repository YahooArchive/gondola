/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola;

public class IndexNotFoundException extends Exception {

    int earliestIndex;

    IndexNotFoundException(int earliestIndex) {
        this.earliestIndex = earliestIndex;
    }

    /**
     * @return the earliest index in the log.
     */
    public int getEarliestIndex() {
        return earliestIndex;
    }
}
