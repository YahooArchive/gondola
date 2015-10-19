package com.yahoo.gondola;

/**
 * This is a global context that holds gondola instance
 */
public class GondolaContext {
    static Gondola gondola;

    public static Gondola getGondola() {
        return gondola;
    }

    public static void setGondola(Gondola gondola) {
        GondolaContext.gondola = gondola;
    }
}
