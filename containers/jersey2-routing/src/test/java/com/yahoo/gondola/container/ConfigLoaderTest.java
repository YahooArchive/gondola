/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import org.testng.annotations.Test;

public class ConfigLoaderTest {

    @Test
    public void testGetConfigInstance() throws Exception {
        /* Sample URIs:
             classpath:///gondola.conf
             file:///usr/local/etc/conf/gondola.conf
             zookeeper://127.0.0.1:2181/serviceName
         */
    }
}