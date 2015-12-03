/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;

public class ConfigWriterTest {
    ConfigWriter configWriter;

    URL url = ConfigWriterTest.class.getClassLoader().getResource("gondola.conf");
    File configFile = new File(url.getFile());

    @BeforeMethod
    public void setUp() throws Exception {
        configWriter = new ConfigWriter(configFile);
    }

    @Test
    public void testSave() throws Exception {
        configWriter.setBucketMap("shard1", "1-1000");
        configWriter.save();
    }

    @Test
    public void testSetBucketMap() throws Exception {
    }
}