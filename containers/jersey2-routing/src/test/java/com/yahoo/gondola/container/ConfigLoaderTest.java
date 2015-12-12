/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.yahoo.gondola.container.utils.ZookeeperServer;

import org.testng.annotations.Test;

public class ConfigLoaderTest {

    ZookeeperServer zookeeperServer = new ZookeeperServer();

    @Test
    public void testGetConfigInstance() throws Exception {
        //ConfigLoader.getConfigInstance(URI.create("classpath:///gondola.conf"));
        //ConfigLoader.getConfigInstance(URI.create("file:///../../../../../resources/gondola.conf"));
        //ConfigLoader.getConfigInstance(URI.create("zookeeper://127.0.0.1:2181/serviceName"));
    }
}