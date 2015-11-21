/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class holds the results from a call to the remote process.
 */
public class ChannelResult {
    static Logger logger = LoggerFactory.getLogger(ChannelResult.class);

    public boolean success = false;
    public String message = "";

    /**
     * Parses the response from a call to the remote process.
     * The returned message is non-null.
     *
     * @param response non-null response.
     */
    public ChannelResult(String response) {
        if (response.startsWith("SUCCESS: ")) {
            success = true;
            message = response.substring(9);
        } else {
            message = response;
        }
    }
}
