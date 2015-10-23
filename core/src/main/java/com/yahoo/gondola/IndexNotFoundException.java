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
